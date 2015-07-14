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
)

const (
	QueueTimeout       = 1
	QueueRetry         = 5
	StoreScanTick      = 3
	ReconcileScanTick  = 5
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
	errChan := make(chan error, 7)
	// Start all worker goroutines
	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task generator", Pending)
		defer sched.wg.Done()
		errChan <- sched.GenerateTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task queuer", Pending)
		defer sched.wg.Done()
		errChan <- sched.QueuePendingTasks()
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
		errChan <- sched.ReconcilePendingTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s reconciler", Doomed)
		defer sched.wg.Done()
		errChan <- sched.ReconcileDoomedTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task cleaner", Doomed)
		defer sched.wg.Done()
		errChan <- sched.DoomCleaner()
	}()

	select {
	case err = <-errChan:
		log.Printf("Scheduler workers finished")
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

func (sched *Scheduler) GenerateTasks() error {
	state := Pending
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var genErr error
	generator:
		for {
			select {
			case <-ticker.C:
				jobs, err := sched.Store.GetJobs(state)
				if err != nil {
					genErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break generator
				}
				for _, job := range jobs {
					tasks, err := sched.Store.GetJobTasks(job.Id)
					if err != nil {
						genErr = fmt.Errorf("Failed to read tasks for Job %s: %s", job.Id, err)
						break generator
					}
					taskCount := job.Task.Replicas - uint32(len(tasks))
					for i := uint32(0); i < taskCount; i++ {
						taskInfo := createMesosTaskInfo(job.Id, job.Task)
						task := &Task{
							Info:  taskInfo,
							JobId: job.Id,
							State: Pending,
						}
						log.Printf("Creating new %s task %s for job %s",
							state, task.Info.TaskId.GetValue(), job.Id)
						if err := sched.Store.AddTask(task); err != nil {
							if serr, ok := err.(*StoreError); ok {
								if serr.Code != ErrExists {
									genErr = fmt.Errorf("Failed to store task %s: %s",
										taskInfo.TaskId.GetValue(), err)
									break generator
								}
							}
						}
					}
				}
			case <-sched.done:
				ticker.Stop()
				genErr = nil
				log.Printf("Finishing Task Generator")
				break generator
			}
		}
		errChan <- genErr
		log.Printf("Task generator ticker stopped")
	}()

	return <-errChan
}

func (sched *Scheduler) QueuePendingTasks() error {
	state := Pending
	queue := Pending.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var qErr error
	queuer:
		for {
			select {
			case <-ticker.C:
				tasks, err := sched.Store.GetTasks(state)
				if err != nil {
					qErr = fmt.Errorf("Error reading %s Tasks: %s", state, err)
					break queuer
				}
				for _, task := range tasks {
					taskId := task.Info.TaskId.GetValue()
					log.Printf("Queueing task %s to %s queue", taskId, queue)
					if err := sched.Queue.Publish(queue, task); err != nil {
						log.Printf("Failed to queue %s: %s", taskId, err)
						continue
					}
				}
			case <-sched.done:
				ticker.Stop()
				qErr = nil
				log.Printf("Finishing %s Task queuer", state)
				break queuer
			}
		}
		errChan <- qErr
		log.Printf("%s tasks queuer ticker stopped", state)
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
	doomer:
		for {
			select {
			case <-ticker.C:
				jobs, err := sched.Store.GetJobs(state)
				if err != nil {
					doomErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break doomer
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := MesosTasks(ctx, sched.master, job.Id,
						mesos.TaskState_TASK_RUNNING.Enum())
					if err != nil {
						log.Printf("Failed to read tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					// Queue tasks for killing
					for taskId, _ := range mesosTasks {
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
					tasks, err := sched.Store.GetJobTasks(job.Id)
					if err != nil {
						doomErr = fmt.Errorf("Failed to read tasks for Job %s: %s", job.Id, err)
						break doomer
					}
					for _, task := range tasks {
						taskId := task.Info.TaskId.GetValue()
						if err := sched.Store.RemoveTask(taskId); err != nil {
							if serr, ok := err.(*StoreError); ok {
								if serr.Code != ErrNotFound {
									doomErr = fmt.Errorf("Failed to remove %s task for job %s: %s",
										taskId, job.Id, err)
									break doomer
								}
							}
							continue
						}
					}
				}
			case <-sched.done:
				ticker.Stop()
				doomErr = nil
				log.Printf("Finished %s task doomer", state)
				break doomer
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
				killErr = nil
				log.Printf("Finishing Task killer")
				break killer
			default:
				var retryCount int
				task, err := sched.Doomed.NextTask(QueueTimeout * time.Second)
				if err != nil {
					switch {
					case sched.Doomed.TimedOut(err):
						log.Printf("No tasks to kill")
					case sched.Doomed.ConnClosed(err):
						killErr = nil
						break
					default:
						retryCount += 1
						log.Printf("Failed to read from %s queue: %s",
							queue, err)

					}
					if retryCount == QueueRetry {
						killErr = fmt.Errorf("Error reading %s queue: %s",
							queue, err)
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
							killErr = fmt.Errorf("Failed to remove task %s: %s",
								taskId.GetValue(), err)
							break killer
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

func (sched *Scheduler) ReconcilePendingTasks() error {
	oldState := Pending
	newState := Scheduled
	queue := Pending.String()
	errChan := make(chan error)
	ticker := time.NewTicker(ReconcileScanTick * time.Second)
	go func() {
		var reconErr error
	reconcile:
		for {
			select {
			case <-ticker.C:
				jobs, err := sched.Store.GetJobs(oldState)
				if err != nil {
					reconErr = fmt.Errorf("Error reading %s Jobs: %s", oldState, err)
					break reconcile
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := MesosTasks(ctx, sched.master, job.Id, nil)
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					launched := len(mesosTasks)
					log.Printf("Launched tasks: %#v", mesosTasks)
					if uint32(launched) == job.Task.Replicas {
						job.State = newState
						if err := sched.Store.UpdateJob(job); err != nil {
							reconErr = fmt.Errorf("Failed to update job %s: %s", job.Id, err)
							break reconcile
						}
						log.Printf("Job %s marked as %s", job.Id, newState)
					} else {
						tasks, err := sched.Store.GetJobTasks(job.Id)
						if err != nil {
							reconErr = fmt.Errorf("Failed to read Tasks for Job %s: %s", job.Id, err)
							break reconcile
						}
						for _, task := range tasks {
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
			case <-sched.done:
				log.Printf("Finished %s reconciler", oldState)
				ticker.Stop()
				reconErr = nil
				break reconcile
			}
		}
		errChan <- reconErr
		log.Printf("%s Reconciler tick stopped", oldState)
	}()

	return <-errChan
}

func (sched *Scheduler) ReconcileDoomedTasks() error {
	oldState := Doomed
	newState := Dead
	queue := Doomed.String()
	errChan := make(chan error)
	ticker := time.NewTicker(ReconcileScanTick * time.Second)
	go func() {
		var reconErr error
	reconcile:
		for {
			select {
			case <-ticker.C:
				jobs, err := sched.Store.GetJobs(oldState)
				if err != nil {
					reconErr = fmt.Errorf("Error reading %s Jobs: %s", oldState, err)
					break reconcile
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := MesosTasks(ctx, sched.master, job.Id,
						mesos.TaskState_TASK_KILLED.Enum())
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					tasks, err := sched.Store.GetJobTasks(job.Id)
					if err != nil {
						reconErr = fmt.Errorf("Failed to read Tasks for Job %s: %s", job.Id, err)
						break reconcile
					}
					launched := len(mesosTasks)
					if uint32(launched) == job.Task.Replicas {
						job.State = newState
						if err := sched.Store.UpdateJob(job); err != nil {
							reconErr = fmt.Errorf("Failed to update job %s: %s", job.Id, err)
							break reconcile
						}
						log.Printf("Job %s marked as %s", job.Id, newState)
					} else {
						for _, task := range tasks {
							// Only queue the ones in old state which have SlaveId populated
							taskId := task.Info.TaskId.GetValue()
							if task.State != newState && task.Info.SlaveId.GetValue() != "" {
								log.Printf("Queueing task %s to %s queue", taskId, queue)
								if err := sched.Queue.Publish(queue, task); err != nil {
									log.Printf("Failed to queue %s: %s", taskId, err)
									continue
								}
							}
						}
					}
				}
			case <-sched.done:
				ticker.Stop()
				reconErr = nil
				log.Printf("Finished %s Job reconciler", oldState)
				break reconcile
			}
		}
		errChan <- reconErr
		log.Printf("%s Reconciler tick stopped", oldState)
	}()

	return <-errChan
}

func (sched *Scheduler) DoomCleaner() error {
	state := Doomed
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		log.Printf("%s Doom cleaner tick started", state)
		var cleanErr error
	cleaner:
		for {
			select {
			case <-ticker.C:
				tasks, err := sched.Store.GetTasks(state)
				if err != nil {
					cleanErr = fmt.Errorf("Failed to read %s Tasks: %s", state, err)
					break cleaner
				}
				for _, task := range tasks {
					taskId := task.Info.TaskId.GetValue()
					if err := sched.Store.RemoveTask(taskId); err != nil {
						if serr, ok := err.(*StoreError); ok {
							if serr.Code != ErrNotFound {
								cleanErr = fmt.Errorf("Failed to remove task %s: %s",
									taskId, err)
								break cleaner
							}
						}
						continue
					}
				}
			case <-sched.done:
				ticker.Stop()
				cleanErr = nil
				log.Printf("Finished %s Cleaner", state)
				break cleaner
			}
		}
		errChan <- cleanErr
		log.Printf("%s Cleaner tick stopped", state)
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

		// Mapp for each launch task batch
		launchTaskMap := make(map[string]bool)
		launchTasks := make([]*mesos.TaskInfo, 0, 0)
		var taskCpu, taskMem float64
		var retryCount int
	ReadTasks:
		for {
			task, err := sched.Pending.NextTask(QueueTimeout * time.Second)
			if err != nil {
				retryCount += 1
				switch {
				case sched.Pending.TimedOut(err):
					log.Printf("No %s tasks available", Pending)
				case sched.Pending.ConnClosed(err):
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
				storeTask, err := sched.Store.GetTask(taskId)
				if err != nil {
					if serr, ok := err.(*StoreError); ok {
						if serr.Code != ErrNotFound {
							sched.errChan <- fmt.Errorf("Failed to read task %s: %s",
								taskId, err)
						}
					}
				}
				if storeTask.State == Doomed {
					log.Printf("Skipping %s task marked as doomed", storeTask.Info.TaskId.GetValue())
					break ReadTasks
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
				storeTask, err := sched.Store.GetTask(taskId)
				if err != nil {
					if serr, ok := err.(*StoreError); ok {
						if serr.Code != ErrNotFound {
							sched.errChan <- fmt.Errorf("Failed to read task %s: %s",
								taskId, err)
						}
					}
				}
				if storeTask.State != Doomed {
					jobId := ParseJobId(taskId)
					task := &Task{
						Info:  launchTask,
						JobId: jobId,
						State: Scheduled,
					}
					if err := sched.Store.UpdateTask(task); err != nil {
						if serr, ok := err.(*StoreError); ok {
							if serr.Code != ErrNotFound {
								sched.errChan <- fmt.Errorf("Failed to update task %s: %s",
									taskId, err)
							}
						}
					}
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
