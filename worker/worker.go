package worker

import (
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"

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
	store  taurus.Store
	queue  taurus.TaskQueue
	doomed taurus.Subscription
	master string
	done   chan struct{}
	wg     sync.WaitGroup
}

func NewBasicTaskWorker(store taurus.Store, queue taurus.TaskQueue) (*BasicTaskWorker, error) {
	done := make(chan struct{})
	doomed, err := queue.Subscribe(taurus.Doomed.String())
	if err != nil {
		return nil, err
	}
	return &BasicTaskWorker{
		store:  store,
		queue:  queue,
		doomed: doomed,
		done:   done,
	}, nil
}

func (tw *BasicTaskWorker) Run(driver scheduler.SchedulerDriver, master string) error {
	tw.master = master
	errChan := make(chan error, 7)

	// Start worker goroutines
	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Task generator")
		defer tw.wg.Done()
		errChan <- tw.GeneratePendingTasks()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Task queuer")
		defer tw.wg.Done()
		errChan <- tw.QueuePendingTasks()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Doomed Task queuer")
		defer tw.wg.Done()
		errChan <- tw.QueueDoomedTasks()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Task killer")
		defer tw.wg.Done()
		errChan <- tw.KillDoomedTasks(driver)
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Doomed Task cleaner")
		defer tw.wg.Done()
		errChan <- tw.CleanDoomedTasks()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Pending Task reconciler")
		defer tw.wg.Done()
		errChan <- tw.ReconcilePendingTasks()
	}()

	tw.wg.Add(1)
	go func() {
		log.Printf("Starting Doomed Task reconciler")
		defer tw.wg.Done()
		errChan <- tw.ReconcileDoomedTasks()
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) KillDoomedTasks(driver scheduler.SchedulerDriver) error {
	queue := taurus.Doomed.String()
	errChan := make(chan error)
	go func() {
		var killErr error
	killer:
		for {
			select {
			case <-tw.done:
				killErr = nil
				log.Printf("Finishing %s Task killer", taurus.Doomed)
				break killer
			default:
				var retryCount int
				task, err := tw.doomed.ReadTask(QueueTimeout * time.Second)
				if err != nil {
					switch {
					case tw.doomed.TimedOut(err):
						log.Printf("No tasks to kill")
					case tw.doomed.Closed(err):
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
				if err := tw.store.RemoveTask(taskId.GetValue()); err != nil {
					if serr, ok := err.(*taurus.StoreError); ok {
						if serr.Code != taurus.ErrNotFound {
							killErr = fmt.Errorf("Failed to remove task %s: %s",
								taskId.GetValue(), err)
							break killer
						}
					}
				}
			}
		}
		errChan <- killErr
		log.Printf("Finished Doom Task killer")
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) GeneratePendingTasks() error {
	state := taurus.Pending
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var genErr error
	generator:
		for {
			select {
			case <-tw.done:
				ticker.Stop()
				genErr = nil
				log.Printf("Finishing %s Task Generator", state)
				break generator
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(state)
				if err != nil {
					genErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break generator
				}
				for _, job := range jobs {
					tasks, err := tw.store.GetJobTasks(job.Id)
					if err != nil {
						genErr = fmt.Errorf("Failed to read tasks for Job %s: %s", job.Id, err)
						break generator
					}
					taskCount := job.Task.Replicas - uint32(len(tasks))
					for i := uint32(0); i < taskCount; i++ {
						taskInfo := taurus.CreateMesosTaskInfo(job.Id, job.Task)
						task := &taurus.Task{
							Info:  taskInfo,
							JobId: job.Id,
							State: taurus.Pending,
						}
						log.Printf("Creating new %s task %s for job %s",
							state, task.Info.TaskId.GetValue(), job.Id)
						if err := tw.store.AddTask(task); err != nil {
							if serr, ok := err.(*taurus.StoreError); ok {
								if serr.Code != taurus.ErrExists {
									genErr = fmt.Errorf("Failed to store task %s: %s",
										taskInfo.TaskId.GetValue(), err)
									break generator
								}
							}
						}
					}
				}
			}
		}
		errChan <- genErr
		log.Printf("%s Task generator ticker stopped", state)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) QueuePendingTasks() error {
	state := taurus.Pending
	queue := taurus.Pending.String()
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
				tasks, err := tw.store.GetTasks(state)
				if err != nil {
					qpErr = fmt.Errorf("Error reading %s Tasks: %s", state, err)
					break queuer
				}
				for _, task := range tasks {
					taskId := task.Info.TaskId.GetValue()
					log.Printf("Queueing task %s to %s queue", taskId, queue)
					if err := tw.queue.Publish(queue, task); err != nil {
						log.Printf("Failed to queue %s: %s", taskId, err)
						continue
					}
				}
			}
		}
		errChan <- qpErr
		log.Printf("%s tasks queuer ticker stopped", state)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) QueueDoomedTasks() error {
	state := taurus.Doomed
	queue := taurus.Doomed.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var doomErr error
	doomer:
		for {
			select {
			case <-tw.done:
				ticker.Stop()
				doomErr = nil
				log.Printf("Finished %s Task queuer", state)
				break doomer
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(state)
				if err != nil {
					doomErr = fmt.Errorf("Error reading %s Jobs: %s", state, err)
					break doomer
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id,
						mesos.TaskState_TASK_RUNNING.Enum())
					if err != nil {
						log.Printf("Failed to read tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					// Queue tasks for killing
					for taskId, _ := range mesosTasks {
						taskInfo := new(mesos.TaskInfo)
						taskInfo.TaskId = mesosutil.NewTaskID(taskId)
						task := &taurus.Task{
							Info:  taskInfo,
							JobId: job.Id,
							State: taurus.Doomed,
						}
						log.Printf("Queueing task %s to %s queue", taskId, queue)
						if err := tw.queue.Publish(queue, task); err != nil {
							log.Printf("Failed to queue %s: %s", taskId, err)
						}
					}
					// Delete Doomed Job tasks
					tasks, err := tw.store.GetJobTasks(job.Id)
					if err != nil {
						doomErr = fmt.Errorf("Failed to read tasks for Job %s: %s", job.Id, err)
						break doomer
					}
					for _, task := range tasks {
						taskId := task.Info.TaskId.GetValue()
						if err := tw.store.RemoveTask(taskId); err != nil {
							if serr, ok := err.(*taurus.StoreError); ok {
								if serr.Code != taurus.ErrNotFound {
									doomErr = fmt.Errorf("Failed to remove %s task for job %s: %s",
										taskId, job.Id, err)
									break doomer
								}
							}
							continue
						}
					}
				}
			}
		}
		errChan <- doomErr
		log.Printf("%s Task queuer ticker stopped", state)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) CleanDoomedTasks() error {
	state := taurus.Doomed
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		var cleanErr error
	cleaner:
		for {
			select {
			case <-tw.done:
				ticker.Stop()
				cleanErr = nil
				log.Printf("Finished %s Task Cleaner", state)
				break cleaner
			case <-ticker.C:
				tasks, err := tw.store.GetTasks(state)
				if err != nil {
					cleanErr = fmt.Errorf("Failed to read %s Tasks: %s", state, err)
					break cleaner
				}
				for _, task := range tasks {
					taskId := task.Info.TaskId.GetValue()
					if err := tw.store.RemoveTask(taskId); err != nil {
						if serr, ok := err.(*taurus.StoreError); ok {
							if serr.Code != taurus.ErrNotFound {
								cleanErr = fmt.Errorf("Failed to remove task %s: %s",
									taskId, err)
								break cleaner
							}
						}
						continue
					}
				}
			}
		}
		errChan <- cleanErr
		log.Printf("%s Task Cleaner tick stopped", state)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) ReconcilePendingTasks() error {
	oldState := taurus.Pending
	newState := taurus.Scheduled
	queue := taurus.Pending.String()
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
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id, nil)
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					launched := len(mesosTasks)
					log.Printf("Launched tasks: %#v", mesosTasks)
					if uint32(launched) == job.Task.Replicas {
						job.State = newState
						if err := tw.store.UpdateJob(job); err != nil {
							reconErr = fmt.Errorf("Failed to update job %s: %s", job.Id, err)
							break reconciler
						}
						log.Printf("Job %s marked as %s", job.Id, newState)
					} else {
						tasks, err := tw.store.GetJobTasks(job.Id)
						if err != nil {
							reconErr = fmt.Errorf("Failed to read Tasks for Job %s: %s", job.Id, err)
							break reconciler
						}
						for _, task := range tasks {
							if task.State != newState {
								taskId := task.Info.TaskId.GetValue()
								log.Printf("Queueing task %s to %s queue", taskId, queue)
								if err := tw.queue.Publish(queue, task); err != nil {
									log.Printf("Failed to queue %s: %s", taskId, err)
									continue
								}
							}
						}
					}
				}
			}
		}
		errChan <- reconErr
		log.Printf("%s Task Reconciler tick stopped", oldState)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) ReconcileDoomedTasks() error {
	oldState := taurus.Doomed
	newState := taurus.Dead
	queue := taurus.Doomed.String()
	errChan := make(chan error)
	ticker := time.NewTicker(ReconcileScanTick * time.Second)
	go func() {
		var reconErr error
	reconciler:
		for {
			select {
			case <-tw.done:
				ticker.Stop()
				reconErr = nil
				log.Printf("Finished %s Task reconciler", oldState)
				break reconciler
			case <-ticker.C:
				jobs, err := tw.store.GetJobs(oldState)
				if err != nil {
					reconErr = fmt.Errorf("Error reading %s Jobs: %s", oldState, err)
					break reconciler
				}
				for _, job := range jobs {
					ctx, cancel := context.WithTimeout(
						context.Background(), QueryMasterTimeout*time.Second)
					mesosTasks, err := taurus.MesosTasks(ctx, tw.master, job.Id,
						mesos.TaskState_TASK_KILLED.Enum())
					if err != nil {
						log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
						cancel()
						continue
					}
					tasks, err := tw.store.GetJobTasks(job.Id)
					if err != nil {
						reconErr = fmt.Errorf("Failed to read Tasks for Job %s: %s", job.Id, err)
						break reconciler
					}
					launched := len(mesosTasks)
					if uint32(launched) == job.Task.Replicas {
						job.State = newState
						if err := tw.store.UpdateJob(job); err != nil {
							reconErr = fmt.Errorf("Failed to update job %s: %s", job.Id, err)
							break reconciler
						}
						log.Printf("Job %s marked as %s", job.Id, newState)
					} else {
						for _, task := range tasks {
							// Only queue the ones in old state which have SlaveId populated
							taskId := task.Info.TaskId.GetValue()
							if task.State != newState && task.Info.SlaveId.GetValue() != "" {
								log.Printf("Queueing task %s to %s queue", taskId, queue)
								if err := tw.queue.Publish(queue, task); err != nil {
									log.Printf("Failed to queue %s: %s", taskId, err)
									continue
								}
							}
						}
					}
				}
			}
		}
		errChan <- reconErr
		log.Printf("%s Task Reconciler tick stopped", oldState)
	}()

	return <-errChan
}

func (tw *BasicTaskWorker) Stop() {
	close(tw.done)
	tw.wg.Wait()
	return
}
