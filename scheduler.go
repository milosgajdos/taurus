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

	return &Scheduler{
		Store:   store,
		Queue:   queue,
		Pending: pending,
		Doomed:  doomed,
		master:  master,
		done:    done,
	}, nil
}

func (sched *Scheduler) Run(driver *sched.MesosSchedulerDriver) error {
	errChan := make(chan error, 3)

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task worker", Pending)
		defer sched.wg.Done()
		errChan <- sched.ScheduleTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s task worker", Doomed)
		defer sched.wg.Done()
		errChan <- sched.DoomTasks()
	}()

	sched.wg.Add(1)
	go func() {
		log.Printf("Starting task killer worker")
		defer sched.wg.Done()
		errChan <- sched.KillTasks(driver)
	}()

	return <-errChan
}

func (sched *Scheduler) Stop() {
	close(sched.done)
	sched.Queue.Close()
	sched.wg.Wait()
	return
}

func (sched *Scheduler) ScheduleTasks() error {
	state, queue := Pending, Pending.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		for range ticker.C {
			jobs, err := sched.Store.GetJobs(state)
			if err != nil {
				errChan <- fmt.Errorf("Error reading new Jobs: %s", err)
			}
			for _, job := range jobs {
				var queued uint32
				for i := uint32(0); i < job.Task.Replicas; i++ {
					taskInfo := createMesosTaskInfo(job.Id, job.Task)
					task := &Task{
						Info:  taskInfo,
						JobId: job.Id,
					}
					if err := sched.Store.AddTask(task); err != nil {
						log.Printf("Failed to store task %s: %s",
							task.Info.TaskId.GetValue(), err)
						continue
					}
					taskId := taskInfo.TaskId.GetValue()
					log.Printf("Queueing task %s to %s queue", taskId, queue)
					if err := sched.Queue.Publish(queue, task); err != nil {
						log.Printf("Failed to queue %s: %s", taskId, err)
						continue
					}
					queued += 1
				}
				job.State = Scheduling
				if err := sched.Store.UpdateJob(job); err != nil {
					errChan <- fmt.Errorf("Failed to update job %s: %s", job.Id, err)
				}
			}
		}
	}()

	select {
	case <-sched.done:
		log.Printf("Finished %s Job scanner", state)
		ticker.Stop()
		return nil
	case err := <-errChan:
		return err
	}
}

func (sched *Scheduler) DoomTasks() error {
	state, queue := Doomed, Doomed.String()
	errChan := make(chan error)
	ticker := time.NewTicker(StoreScanTick * time.Second)
	go func() {
		for range ticker.C {
			jobs, err := sched.Store.GetJobs(state)
			if err != nil {
				errChan <- fmt.Errorf("Error reading new Jobs: %s", err)
			}
			for _, job := range jobs {
				ctx, cancel := context.WithTimeout(
					context.Background(), QueryMasterTimeout*time.Second)
				taskIds, err := MesosTaskIds(ctx, sched.master, job.Id,
					mesos.TaskState_TASK_RUNNING.Enum())
				if err != nil {
					log.Printf("Failed to retrieve Tasks for Job %s: %s", job.Id, err)
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
					}
					log.Printf("Queueing task %s to %s queue", taskId, queue)
					if err := sched.Queue.Publish(queue, task); err != nil {
						log.Printf("Failed to queue %s: %s", taskId, err)
						continue
					}
				}
				// Delete Doomed tasks which haven't been launched yet
				tasks, err := sched.Store.GetTasks(job.Id)
				if err != nil {
					log.Printf("Failed to read Tasks for Job %s: %s", job.Id, err)
				}
				for _, task := range tasks {
					taskId := task.Info.TaskId.GetValue()
					if err := sched.Store.RemoveTask(taskId); err != nil {
						log.Printf("Failed to remove %s task for job %s: %s",
							taskId, job.Id, err)
						continue
					}
					log.Printf("Removed %s task %s", state, taskId)
				}

				job.State = Killing
				if err := sched.Store.UpdateJob(job); err != nil {
					errChan <- fmt.Errorf("Failed to update job %s: %s", job.Id, err)
				}
			}
		}
	}()

	select {
	case <-sched.done:
		log.Printf("Finished %s Job scanner", state)
		ticker.Stop()
		return nil
	case err := <-errChan:
		return err
	}
}

func (sched *Scheduler) KillTasks(driver *sched.MesosSchedulerDriver) error {
	queue := Doomed.String()
	errChan := make(chan error)
	go func() {
		for {
			var retryCount int
			task, err := sched.Doomed.NextTask(QueueTimeout * time.Second)
			if err != nil {
				switch err {
				case nats.ErrTimeout:
					log.Printf("No tasks to kill")
				case nats.ErrConnectionClosed:
					errChan <- nil
				default:
					retryCount += 1
					log.Printf("Failed to read from %s queue: %s", queue, err)
				}
				if retryCount == QueueRetry {
					errChan <- fmt.Errorf("Error reading %s queue: %s", queue, err)
				}
				continue
			}
			killStatus, err := driver.KillTask(task.Info.TaskId)
			if err != nil {
				log.Printf("Mesos in state %s failed to kill the task %s: %s",
					killStatus, task.Info.TaskId.GetValue(), err)
			}
		}
	}()

	select {
	case <-sched.done:
		log.Printf("Finished Task killer")
		return nil
	case err := <-errChan:
		return err
	}
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
					log.Printf("No pending tasks available")
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
				taskCpu = ScalarResourceVal("cpus", task.Info.Resources)
				taskMem = ScalarResourceVal("mem", task.Info.Resources)
				if remainingCpus >= taskCpu && remainingMems >= taskMem {
					task.Info.SlaveId = offer.SlaveId
					launchTasks = append(launchTasks, task.Info)
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
			for _, task := range launchTasks {
				taskId := task.TaskId.GetValue()
				if err := sched.Store.RemoveTask(taskId); err != nil {
					log.Printf("Failed to remove task %s: %s", taskId, err)
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
	log.Println("Scheduler received error:", err)
}
