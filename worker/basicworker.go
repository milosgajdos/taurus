package worker

import (
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/milosgajdos83/taurus"
)

const (
	Pending = "pending"
)

const (
	QueueTimeout      = 1 * time.Second
	MasterTimeout     = 2 * time.Second
	StoreScanTick     = 3 * time.Second
	ReconcileScanTick = 5 * time.Second
	QueueRetry        = 5
)

// BasicWorker provides a basic implementation of taurus.Worker interface
//
// BasickWorker does all the Job management heavy lifting.
// It starts and manages multiple goroutines responsible for monitoring Job states, queueing, launching and killing Job Tasks using Mesos.
type BasicWorker struct {
	store   taurus.Store
	queue   taurus.TaskQueue
	pending taurus.Subscription
	master  string
	done    chan struct{}
	wg      sync.WaitGroup
}

// NewBasicWorker initializes BasicWorker
//
// It returns error if the BasicWorker could not be initialized. This can be due to issues with TaskQueue or Store
func NewBasicWorker(store taurus.Store, queue taurus.TaskQueue) (*BasicWorker, error) {
	pending, err := queue.Subscribe(Pending)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	return &BasicWorker{
		store:   store,
		queue:   queue,
		pending: pending,
		done:    done,
	}, nil
}

// Start starts several BasicWorker goroutines responsible for launching and killing the Tasks
//
// Start blocks waiting to receive an error if any of the Worker goroutines fails with error
func (bw *BasicWorker) Start(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) error {
	bw.master = taurus.MasterConnStr(masterInfo)
	errChan := make(chan error, 3)

	// Start worker goroutines
	bw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Task queuer")
		defer bw.wg.Done()
		errChan <- bw.QueuePendingTasks()
	}()

	bw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Jobs reconciler")
		defer bw.wg.Done()
		errChan <- bw.ReconcilePendingJobs()
	}()

	bw.wg.Add(1)
	go func() {
		log.Printf("Starting Task Killer")
		defer bw.wg.Done()
		errChan <- bw.KillJobTasks(driver)
	}()

	return <-errChan
}

// Schedule handles Mesos resource offers and based on the received offers launches PENDING Tasks
//
// It monitors PENDING Tasks Queue and tries to launch as many Tasks as possible for a single offer.
// If no PENDING Tasks are available within QueueTimeout interval it declines the offer and retries Queue read again
func (bw *BasicWorker) ScheduleTasks(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
ReadOffers:
	for _, offer := range offers {
		remainingCpus := taurus.ScalarResourceVal("cpus", offer.Resources)
		remainingMems := taurus.ScalarResourceVal("mem", offer.Resources)

		log.Println("Taurus Received Offer <", offer.Id.GetValue(), "> with cpus=", remainingCpus, " mem=", remainingMems)

		// Map to avoid launching duplicate tasks in the same batch slice
		launchTaskMap := make(map[string]bool)
		launchTasks := make([]*mesos.TaskInfo, 0, 0)
		var taskCpu, taskMem float64
		var retryCount int
	ReadTasks:
		for {
			task, err := bw.pending.ReadTask(QueueTimeout)
			if err != nil {
				retryCount += 1
				switch {
				case bw.pending.TimedOut(err):
					log.Printf("No %s tasks available", taurus.PENDING)
				case bw.pending.Closed(err):
					break ReadTasks
				default:
					log.Printf("Failed to read from %s queue: %s", taurus.PENDING, err)
				}
				if retryCount == QueueRetry {
					break ReadTasks
				}
				continue ReadTasks
			}
			if task != nil {
				taskId := task.Info.TaskId.GetValue()
				// Don't add the same task bwice into launchTasks slice
				if launchTaskMap[taskId] {
					log.Printf("Skipping already queued Task %s", taskId)
					continue ReadTasks
				}
				taskCpu = taurus.ScalarResourceVal("cpus", task.Info.Resources)
				taskMem = taurus.ScalarResourceVal("mem", task.Info.Resources)
				if remainingCpus >= taskCpu && remainingMems >= taskMem {
					task.Info.SlaveId = offer.SlaveId
					launchTasks = append(launchTasks, task.Info)
					launchTaskMap[taskId] = true
					remainingCpus -= taskCpu
					remainingMems -= taskMem
				} else {
					break ReadTasks
				}
			}
		}

		if len(launchTasks) > 0 {
			log.Printf("Launching %d tasks for offer %s", len(launchTasks), offer.Id.GetValue())
			launchStatus, err := driver.LaunchTasks(
				[]*mesos.OfferID{offer.Id},
				launchTasks,
				&mesos.Filters{RefuseSeconds: proto.Float64(1)})
			if err != nil {
				log.Printf("Mesos status: %#v Failed to launch Tasks %s: %s", launchStatus, launchTasks, err)
				continue ReadOffers
			}
		} else {
			log.Println("Declining offer ", offer.Id.GetValue())
			declineStatus, err := driver.DeclineOffer(
				offer.Id,
				&mesos.Filters{RefuseSeconds: proto.Float64(1)})
			if err != nil {
				log.Printf("Error declining offer for mesos status %#v: %s", declineStatus, err)
			}
		}
	}
}

// StatusUpdate handles status updates messages received from Mesos master
//
// Currently this method only logs status updates. This might change in the future
func (bw *BasicWorker) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	taskId := status.TaskId.GetValue()
	taskStatus := status.GetState()
	log.Println("Task", taskId, "is in state", taskStatus.String())

	switch taskStatus {
	case mesos.TaskState_TASK_RUNNING:
		log.Printf("Marking task %s as %s", taskId, taurus.RUNNING)
	case mesos.TaskState_TASK_KILLED, mesos.TaskState_TASK_FINISHED,
		mesos.TaskState_TASK_FAILED, mesos.TaskState_TASK_LOST:
		log.Printf("Marking task %s as %s", taskId, taurus.STOPPED)
	}
}

// Stop stops all BasicWorker goroutines
//
// Stop waits for all worker goroutines to cleanly stop and then returns
func (bw *BasicWorker) Stop() {
	bw.queue.Close()
	close(bw.done)
	bw.wg.Wait()
	return
}

// QueuePendingTasks watches PENDING Job Store, generates appropriate Tasks and queues them into Pending Tasks queue
//
// QueuePendingTasks runs in a separate goroutine started in Worker.Start call
// It returns error if it can't read Job Store or if the goroutine it's running in has been stopped.
func (bw *BasicWorker) QueuePendingTasks() error {
	state := taurus.PENDING
	queue := Pending
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick)
	go func() {
		var qpErr error
	queuer:
		for {
			select {
			case <-bw.done:
				ticker.Stop()
				qpErr = nil
				log.Printf("Finishing %s Task queuer", state)
				break queuer
			case <-ticker.C:
				jobs, err := bw.store.GetJobs(state)
				if err != nil {
					qpErr = fmt.Errorf("Error reading new Jobs: %s", err)
					break queuer
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), MasterTimeout)
					launchedTasks, err := taurus.MesosTasks(ctx, bw.master, job.Id, nil)
					log.Printf("Job %s has %d launched tasks", job.Id, len(launchedTasks))
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					for _, jobTask := range job.Tasks {
						for i := uint32(0); i < jobTask.Replicas-uint32(len(launchedTasks)); i++ {
							taskInfo := taurus.CreateMesosTaskInfo(job.Id, jobTask)
							task := &taurus.Task{
								Info:  taskInfo,
								JobId: job.Id,
							}
							taskId := taskInfo.TaskId.GetValue()
							log.Printf("Queueing task: %s", taskId)
							if err := bw.queue.Publish(queue, task); err != nil {
								log.Printf("Failed to queue %s: %s", taskId, err)
								continue
							}
						}
					}
				}
			}
		}
		errChan <- qpErr
		log.Printf("%s tasks queuer ticker stopped", state)
	}()

	return <-errChan
}

// ReconcilePendingJobs monitors launched Tasks of each PENDING Job and marks the Job as RUNNING
// if all of the Job tasks have been attempted to launch
//
// ReconcilePendingJobs runs in a separate goroutine started in Worker.Start call
// It returns error if it can't read the Job Store or the gourine it is running in has been stopped
func (bw *BasicWorker) ReconcilePendingJobs() error {
	oldState := taurus.PENDING
	newState := taurus.RUNNING
	errChan := make(chan error)
	ticker := time.NewTicker(ReconcileScanTick)
	go func() {
		var reconErr error
	reconciler:
		for {
			select {
			case <-bw.done:
				log.Printf("Finished %s Reconciler", oldState)
				ticker.Stop()
				reconErr = nil
				break reconciler
			case <-ticker.C:
				jobs, err := bw.store.GetJobs(oldState)
				if err != nil {
					reconErr = fmt.Errorf("Error reading %s Jobs: %s", oldState, err)
					break reconciler
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), MasterTimeout)
					launchedTasks, err := taurus.MesosTasks(ctx, bw.master, job.Id, nil)
					log.Printf("Job %s has %d launched tasks", job.Id, len(launchedTasks))
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					jobTaskCount := uint32(0)
					for _, jobTask := range job.Tasks {
						jobTaskCount += jobTask.Replicas
					}
					if uint32(len(launchedTasks)) == jobTaskCount {
						job.State = newState
						if err := bw.store.UpdateJob(job); err != nil {
							reconErr = fmt.Errorf("Failed to update job %s: %s", job.Id, err)
							break reconciler
						}
						log.Printf("Job %s marked as %s", job.Id, newState)
					}
				}
			}
		}
		errChan <- reconErr
		log.Printf("%s Task Reconciler tick stopped", oldState)
	}()

	return <-errChan
}

// KillJobTasks monitors all Jobs marked as STOPPED and kills all of their running Tasks
//
// KillJobTasks runs in a separate goroutine started in Worker.Start call
// It returns error if it can't read Job Store or the goroutine it is running in has been stopped
func (bw *BasicWorker) KillJobTasks(driver scheduler.SchedulerDriver) error {
	state := taurus.STOPPED
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick)
	go func() {
		var killErr error
	killer:
		for {
			select {
			case <-bw.done:
				ticker.Stop()
				killErr = nil
				log.Printf("Finishing %s Task queuer", state)
				break killer
			case <-ticker.C:
				jobs, err := bw.store.GetJobs(state)
				if err != nil {
					killErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break killer
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), MasterTimeout)
					mesosTasks, err := taurus.MesosTasks(ctx, bw.master, job.Id, mesos.TaskState_TASK_RUNNING.Enum())
					if err != nil {
						log.Printf("Failed to read tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					for taskId, _ := range mesosTasks {
						mesosTaskId := mesosutil.NewTaskID(taskId)
						killStatus, err := driver.KillTask(mesosTaskId)
						if err != nil {
							log.Printf("Mesos in state %s failed to kill the task %s: %s", killStatus, taskId, err)
							continue
						}
					}
				}
			}
		}
		errChan <- killErr
		log.Printf("%s tasks killer ticker stopped", state)
	}()

	return <-errChan
}
