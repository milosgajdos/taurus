package taurus

import (
	"fmt"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	QueueTimeout = 1
	QueueRetry   = 5
)

type Scheduler struct {
	Store   Store
	Queue   TaskQueue
	Worker  TaskWorker
	Pending Subscription
	master  string
	errChan chan error
}

func NewScheduler(store Store, queue TaskQueue, worker TaskWorker, master string) (*Scheduler, error) {
	pending, err := queue.Subscribe(Pending.String())
	if err != nil {
		return nil, err
	}
	errChan := make(chan error)

	return &Scheduler{
		Store:   store,
		Queue:   queue,
		Worker:  worker,
		Pending: pending,
		master:  master,
		errChan: errChan,
	}, nil
}

func (sched *Scheduler) Run(driver *sched.MesosSchedulerDriver) (err error) {
	select {
	case err = <-sched.Worker.Run(driver):
		log.Printf("Scheduler workers finished")
	case err = <-sched.errChan:
		log.Printf("Mesos driver failed")
	}

	return err
}

func (sched *Scheduler) Stop() {
	sched.Queue.Close()
	sched.Worker.Stop()
	return
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
			task, err := sched.Pending.ReadTask(QueueTimeout * time.Second)
			if err != nil {
				retryCount += 1
				switch {
				case sched.Pending.TimedOut(err):
					log.Printf("No %s tasks available", Pending)
				case sched.Pending.Closed(err):
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
