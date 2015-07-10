package taurus

import (
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/nats-io/nats"
)

const (
	QueueTimeout       = 1
	QueueRetry         = 5
	StoreScanTick      = 3
	QueryMasterTimeout = 2
)

type Scheduler struct {
	Store   Store
	Queue   Queue
	Pending Subscription
	Doomed  Subscription
	master  string
	done    chan struct{}
	errChan chan error
	wg      sync.WaitGroup
}

func NewScheduler(store Store, queue Queue, master string) (*Scheduler, error) {
	pending, err := queue.Subscribe(Pending.String())
	if err != nil {
		return nil, err
	}
	doomed, err := queue.Subscribe(Doomed.String())
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	errChan := make(chan error)

	return &Scheduler{
		Store:   store,
		Queue:   queue,
		Pending: pending,
		Doomed:  doomed,
		master:  master,
		done:    done,
		errChan: errChan,
	}, nil
}

func (sched *Scheduler) Run(driver *sched.MesosSchedulerDriver) (err error) {
	errChan := make(chan error, 5)
	// Start all worker goroutines
	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task scheduler", Pending)
		defer sched.wg.Done()
		errChan <- sched.ScheduleTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task queuer", Doomed)
		defer sched.wg.Done()
		errChan <- sched.DoomTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting Task killer")
		defer sched.wg.Done()
		errChan <- sched.KillTasks(driver)
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s reconciler", Pending)
		defer sched.wg.Done()
		errChan <- sched.ReconcileScheduling()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s reconciler", Doomed)
		defer sched.wg.Done()
		errChan <- sched.ReconcileKilling()
	}()

	select {
	case err = <-errChan:
		log.Printf("Taurus worker finished")
	case err = <-sched.errChan:
		log.Printf("Mesos driver failed")
	}

	return err
}

func (sched *Scheduler) Stop() {
	close(sched.done)
	sched.Queue.Close()
	sched.wg.Wait()
	return
}

func (sched *Scheduler) ScheduleTasks() error {
	state := Pending
	queue := Pending.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var schedErr error
	scanner:
		for {
			select {
			case <-ticker.C:
				jobs, err := sched.Store.GetJobs(state)
				if err != nil {
					schedErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break scanner
				}
				for _, job := range jobs {
					for i := uint32(0); i < job.Task.Replicas; i++ {
						taskInfo := createMesosTaskInfo(job.Id, job.Task)
						task := &Task{
							Info:  taskInfo,
							JobId: job.Id,
							State: state,
						}
						taskId := taskInfo.TaskId.GetValue()
						if err := sched.Store.AddTask(task); err != nil {
							log.Printf("Failed to store task %s: %s",
								taskId, err)
							continue
						}
						log.Printf("Queueing task %s to %s queue", taskId, queue)
						if err := sched.Queue.Publish(queue, task); err != nil {
							log.Printf("Failed to queue %s: %s", taskId, err)
							continue
						}
					}
				}
			case <-sched.done:
				log.Printf("Finished %s Job scanner", state)
				ticker.Stop()
				schedErr = nil
				break scanner
			}
		}
		errChan <- schedErr
		log.Printf("%s tasks ticker stopped", state)
	}()

	return <-errChan
}

func (sched *Scheduler) DoomTasks() error {
	state := Doomed
	queue := Doomed.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var doomErr error
	scanner:
		for {
			select {
			case <-ticker.C:
				jobs, err := sched.Store.GetJobs(state)
				if err != nil {
					doomErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break scanner
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					taskIds, err := MesosTaskIds(ctx, sched.master, job.Id,
						mesos.TaskState_TASK_RUNNING.Enum())
					if err != nil {
						log.Printf("Failed to read tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					// Queue tasks for killing
					for _, taskId := range taskIds {
						taskInfo := new(mesos.TaskInfo)
						taskInfo.TaskId = util.NewTaskID(taskId)
						task := &Task{
							Info:  taskInfo,
							JobId: job.Id,
							State: Doomed,
						}
						log.Printf("Queueing task %s to %s queue", taskId, queue)
						if err := sched.Queue.Publish(queue, task); err != nil {
							log.Printf("Failed to queue %s: %s", taskId, err)
						}
					}
					// Delete Doomed Job tasks
					tasks, err := sched.Store.GetTasks(job.Id)
					if err != nil {
						log.Printf("Failed to read tasks for Job %s: %s", job.Id, err)
					}
					for _, task := range tasks {
						taskId := task.Info.TaskId.GetValue()
						if err := sched.Store.RemoveTask(taskId); err != nil {
							if serr, ok := err.(*StoreError); ok {
								if serr.Code != ErrNotFound {
									log.Printf("Failed to remove %s task for job %s: %s",
										taskId, job.Id, err)
								}
							}
							continue
						}
					}
				}
			case <-sched.done:
				log.Printf("Finished %s Job scanner", state)
				ticker.Stop()
				doomErr = nil
				break scanner
			}
		}
		errChan <- doomErr
		log.Printf("%s tasks ticker stopped", state)
	}()

	return <-errChan
}

func (sched *Scheduler) KillTasks(driver *sched.MesosSchedulerDriver) error {
	queue := Doomed.String()
	errChan := make(chan error)
	go func() {
		var killErr error
	killer:
		for {
			select {
			case <-sched.done:
				log.Printf("Finishing Task killer")
				killErr = nil
				break killer
			default:
				var retryCount int
				task, err := sched.Doomed.NextTask(QueueTimeout * time.Second)
				if err != nil {
					switch err {
					case nats.ErrTimeout:
						log.Printf("No tasks to kill")
					case nats.ErrConnectionClosed:
						killErr = nil
						break
					default:
						retryCount += 1
						log.Printf("Failed to read from %s queue: %s", queue, err)
					}
					if retryCount == QueueRetry {
						killErr = fmt.Errorf("Error reading %s queue: %s", queue, err)
						break killer
					}
					continue
				}
				taskId := task.Info.TaskId
				killStatus, err := driver.KillTask(taskId)
				if err != nil {
					log.Printf("Mesos in state %s failed to kill the task %s: %s",
						killStatus, taskId.GetValue(), err)
					continue
				}
				if err := sched.Store.RemoveTask(taskId.GetValue()); err != nil {
					if serr, ok := err.(*StoreError); ok {
						if serr.Code != ErrNotFound {
							log.Printf("Failed to remove task %s: %s",
								taskId.GetValue(), err)
						}
					}
				}
			}
		}
		errChan <- killErr
		log.Printf("Finished Task killer")
	}()

	return <-errChan
}

func (sched *Scheduler) reconcileJobs(currState, newState State, queue string, taskState *mesos.TaskState) error {
	jobs, err := sched.Store.GetJobs(currState)
	if err != nil {
		return fmt.Errorf("Error reading %s Jobs: %s", currState, err)
	}
	for _, job := range jobs {
		ctx, cancel := context.WithTimeout(
			context.Background(), QueryMasterTimeout*time.Second)
		taskIds, err := MesosTaskIds(ctx, sched.master, job.Id, taskState)
		if err != nil {
			log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
			cancel()
			continue
		}
		launched := len(taskIds)
		tasks, err := sched.Store.GetTasks(job.Id)
		if err != nil {
			log.Printf("Failed to read Tasks for Job %s: %s", job.Id, err)
			continue
		}
		if uint32(launched) == job.Task.Replicas {
			for _, task := range tasks {
				taskId := task.Info.TaskId.GetValue()
				task.State = newState
				if err := sched.Store.UpdateTask(task); err != nil {
					log.Printf("Failed to update %s task for job %s: %s",
						task.Info.TaskId.GetValue(), job.Id, err)
					continue
				}
				log.Printf("Updated task %s to state: %s", taskId, Scheduled)
			}
			job.State = newState
			if err := sched.Store.UpdateJob(job); err != nil {
				return fmt.Errorf("Failed to update job %s: %s", job.Id, err)
			}
		} else {
			for _, task := range tasks {
				// Only queue the ones in previous state
				if task.State != newState {
					taskId := task.Info.TaskId.GetValue()
					log.Printf("Queueing task %s to %s queue", taskId, queue)
					if err := sched.Queue.Publish(queue, task); err != nil {
						log.Printf("Failed to queue %s: %s", taskId, err)
						continue
					}
				}
			}
		}
	}
	return nil
}

func (sched *Scheduler) ReconcileScheduling() error {
	currJobState := Pending
	newJobState := Scheduled
	queue := Pending.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		log.Printf("%s Reconciler tick started", currJobState)
		var reconErr error
	reconcile:
		for {
			select {
			case <-ticker.C:
				reconErr = sched.reconcileJobs(currJobState, newJobState, queue, nil)
				if reconErr != nil {
					break reconcile
				}
			case <-sched.done:
				log.Printf("Finished %s Job reconciler", currJobState)
				ticker.Stop()
				reconErr = nil
				break reconcile
			}
		}
		errChan <- reconErr
		log.Printf("%s Reconciler tick stopped", currJobState)
	}()

	return <-errChan
}

func (sched *Scheduler) ReconcileKilling() error {
	currJobState := Doomed
	newJobState := Dead
	queue := Doomed.String()
	taskState := mesos.TaskState_TASK_KILLED.Enum()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		log.Printf("%s Reconciler tick started", currJobState)
		var reconErr error
	reconcile:
		for {
			select {
			case <-ticker.C:
				reconErr = sched.reconcileJobs(currJobState, newJobState, queue, taskState)
				if reconErr != nil {
					break reconcile
				}
			case <-sched.done:
				log.Printf("Finished %s Job reconciler", currJobState)
				ticker.Stop()
				reconErr = nil
				break reconcile
			}
		}
		errChan <- reconErr
		log.Printf("%s Reconciler tick stopped", currJobState)
	}()

	return <-errChan
}

// These implement mesos.Scheduler interface
func (sched *Scheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Println("Taurus Framework Registered with Master", masterInfo)
	sched.master = MasterConnStr(masterInfo)
}

func (sched *Scheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	//TODO: We must reconcile the JobTasks here with what is in the store
	log.Println("Taurus Framework Re-Registered with Master", masterInfo)
	sched.master = MasterConnStr(masterInfo)
}

func (sched *Scheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Taurus Scheduler Disconnected from Master")
}

func (sched *Scheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
ReadOffers:
	for _, offer := range offers {
		remainingCpus := ScalarResourceVal("cpus", offer.Resources)
		remainingMems := ScalarResourceVal("mem", offer.Resources)

		log.Println("Taurus Received Offer <", offer.Id.GetValue(), "> with cpus=", remainingCpus, " mem=", remainingMems)

		launchTaskMap := make(map[string]bool)
		launchTasks := make([]*mesos.TaskInfo, 0, 0)
		var taskCpu, taskMem float64
		var retryCount int
	ReadTasks:
		for {
			task, err := sched.Pending.NextTask(QueueTimeout * time.Second)
			if err != nil {
				retryCount += 1
				switch err {
				case nats.ErrTimeout:
					log.Printf("No %s tasks available", Pending)
				case nats.ErrConnectionClosed:
					break ReadTasks
				default:
					log.Printf("Failed to read from %s queue: %s", Pending, err)
				}
				if retryCount == QueueRetry {
					break ReadTasks
				}
				continue ReadTasks
			}
			if task != nil {
				taskId := task.Info.TaskId.GetValue()
				// Don't add the same task twice intu launchTasks slice
				if launchTaskMap[taskId] {
					log.Printf("Skipping already queued Task %s", taskId)
					continue ReadTasks
				}
				taskCpu = ScalarResourceVal("cpus", task.Info.Resources)
				taskMem = ScalarResourceVal("mem", task.Info.Resources)
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
			log.Printf("Launching %d tasks for offer %s",
				len(launchTasks), offer.Id.GetValue())
			launchStatus, err := driver.LaunchTasks(
				[]*mesos.OfferID{offer.Id},
				launchTasks,
				&mesos.Filters{RefuseSeconds: proto.Float64(1)})
			if err != nil {
				log.Printf("Mesos status: %#v Failed to launch Tasks %s: %s",
					launchStatus, launchTasks, err)
				continue ReadOffers
			}
			for _, launchTask := range launchTasks {
				taskId := launchTask.TaskId.GetValue()
				jobId := ParseJobId(taskId)
				task := &Task{
					Info:  launchTask,
					JobId: jobId,
					State: Scheduled,
				}
				if err := sched.Store.UpdateTask(task); err != nil {
					log.Printf("Failed to update task %s: %s", taskId, err)
				}
			}
		} else {
			log.Println("Declining offer ", offer.Id.GetValue())
			declineStatus, err := driver.DeclineOffer(
				offer.Id,
				&mesos.Filters{RefuseSeconds: proto.Float64(1)})
			if err != nil {
				log.Printf("Error declining offer for mesos status %#v: %s",
					declineStatus, err)
			}
		}
	}
}

func (sched *Scheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Println("Task", status.TaskId.GetValue(), "is in state", status.State.Enum().String())
}

func (sched *Scheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

func (sched *Scheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

func (sched *Scheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}

func (sched *Scheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {}

func (sched *Scheduler) Error(driver sched.SchedulerDriver, err string) {
	sched.errChan <- fmt.Errorf("cheduler received error: %s", err)
}
