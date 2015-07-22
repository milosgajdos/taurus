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
	QueueTimeout       = 1
	QueueRetry         = 5
	StoreScanTick      = 3
	ReconcileScanTick  = 5
	QueryMasterTimeout = 2
)

type BasicTaskWorker struct {
	store   taurus.Store
	queue   taurus.TaskQueue
	pending taurus.Subscription
	master  string
	done    chan struct{}
	wg      sync.WaitGroup
}

func NewBasicTaskWorker(store taurus.Store, queue taurus.TaskQueue) (*BasicTaskWorker, error) {
	pending, err := queue.Subscribe(taurus.PendingQ)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	return &BasicTaskWorker{
		store:   store,
		queue:   queue,
		pending: pending,
		done:    done,
	}, nil
}

// Run starts several BasicWorker goroutines
// It blocks waiting to receive an error if any of the workers goroutines fail
func (tw *BasicTaskWorker) Start(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) error {
	tw.master = taurus.MasterConnStr(masterInfo)
	errChan := make(chan error, 3)

	// Start worker goroutines
	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Task queuer")
		defer tw.wg.Done()
		errChan <- tw.QueuePendingTasks()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Task reconciler")
		defer tw.wg.Done()
		errChan <- tw.ReconcilePendingJobs()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Task Killer")
		defer tw.wg.Done()
		errChan <- tw.KillJobTasks(driver)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) Schedule(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
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
			task, err := tw.pending.ReadTask(QueueTimeout * time.Second)
			if err != nil {
				retryCount += 1
				switch {
				case tw.pending.TimedOut(err):
					log.Printf("No %s tasks available", taurus.PENDING)
				case tw.pending.Closed(err):
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
				// Don't add the same task twice into launchTasks slice
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

// Stop stops all BasicWorker goroutines
// It waits for all worker goroutines to stopand then returns
func (tw *BasicTaskWorker) Stop() {
	close(tw.done)
	tw.wg.Wait()
	return
}

// QueuePendingTasks watches for PENDING tasks in Taurus store
// and queues them into Scheduler's queue for launch
func (tw *BasicTaskWorker) QueuePendingTasks() error {
	state := taurus.PENDING
	queue := taurus.PendingQ
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var qpErr error
	queuer:
		for {
			select {
			case <-tw.done:
				ticker.Stop()
				qpErr = nil
				log.Printf("Finishing %s Task queuer", state)
				break queuer
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(state)
				if err != nil {
					qpErr = fmt.Errorf("Error reading new Jobs: %s", err)
					break queuer
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), QueryMasterTimeout*time.Second)
					launchedTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id, nil)
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
							if err := tw.queue.Publish(queue, task); err != nil {
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

// ReconcilePendingJobs monitors launched Taurus Tasks for each PENDING Job
// If all required Tasks have been launched, the Taurus Job is marked as RUNNING
func (tw *BasicTaskWorker) ReconcilePendingJobs() error {
	oldState := taurus.PENDING
	newState := taurus.RUNNING
	errChan := make(chan error)
	ticker := time.NewTicker(ReconcileScanTick * time.Second)
	go func() {
		var reconErr error
	reconciler:
		for {
			select {
			case <-tw.done:
				log.Printf("Finished %s Reconciler", oldState)
				ticker.Stop()
				reconErr = nil
				break reconciler
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(oldState)
				if err != nil {
					reconErr = fmt.Errorf("Error reading %s Jobs: %s", oldState, err)
					break reconciler
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), QueryMasterTimeout*time.Second)
					launchedTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id, nil)
					log.Printf("Job %s has %s launched tasks", job.Id, len(launchedTasks))
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
						if err := tw.store.UpdateJob(job); err != nil {
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

// KillJobTasks monitors all jobs marked as stopped and kills all of its running Tasks
func (tw *BasicTaskWorker) KillJobTasks(driver scheduler.SchedulerDriver) error {
	state := taurus.STOPPED
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var killErr error
	killer:
		for {
			select {
			case <-tw.done:
				ticker.Stop()
				killErr = nil
				log.Printf("Finishing %s Task queuer", state)
				break killer
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(state)
				if err != nil {
					killErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break killer
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id, mesos.TaskState_TASK_RUNNING.Enum())
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

// ReconcileStoppedJobs monitors killed Taurus Tasks for each RUNNING Job
// If all required Tasks have been killed, the Taurus Job is marked as STOPPED
func (tw *BasicTaskWorker) ReconcileSoppedJobs() error {
	oldState := taurus.RUNNING
	newState := taurus.STOPPED
	errChan := make(chan error)
	ticker := time.NewTicker(ReconcileScanTick * time.Second)
	go func() {
		var reconErr error
	reconciler:
		for {
			select {
			case <-tw.done:
				log.Printf("Finished %s Reconciler", oldState)
				ticker.Stop()
				reconErr = nil
				break reconciler
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(oldState)
				if err != nil {
					reconErr = fmt.Errorf("Error reading %s Jobs: %s", oldState, err)
					break reconciler
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), QueryMasterTimeout*time.Second)
					killedTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id, mesos.TaskState_TASK_KILLED.Enum())
					log.Printf("Job %s has %s killed tasks", job.Id, len(killedTasks))
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					jobTaskCount := uint32(0)
					for _, jobTask := range job.Tasks {
						jobTaskCount += jobTask.Replicas
					}
					if uint32(len(killedTasks)) == jobTaskCount {
						job.State = newState
						if err := tw.store.UpdateJob(job); err != nil {
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
