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
	Worker  JobWorker
	Pending Subscription
	master  string
	errChan chan error
}

func NewScheduler(store Store, queue TaskQueue, worker JobWorker, master string) (*Scheduler, error) {
	pending, err := queue.Subscribe(PendingQ)
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
	return <-sched.errChan
}

func (sched *Scheduler) Stop() {
	sched.Queue.Close()
	sched.Worker.Stop()
	return
}

// These implement mesos.Scheduler interface
func (sched *Scheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	sched.master = MasterConnStr(masterInfo)
	log.Println("Taurus Framework Registered with Master", sched.master)
	// Start the scheduler worker
	go func() {
		log.Printf("Starting %s framework scheduler worker", FrameworkName)
		sched.errChan <- sched.Worker.Run(driver, masterInfo)
	}()
}

func (sched *Scheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	//TODO: We must reconcile the Job Tasks here with what is in the store
	// Stop scheduler worker
	sched.Worker.Stop()
	sched.master = MasterConnStr(masterInfo)
	log.Println("Taurus Framework Re-Registered with Master", sched.master)
	// Restart scheduler worker
	go func() {
		log.Printf("Starting %s framework scheduler worker", FrameworkName)
		sched.errChan <- sched.Worker.Run(driver, masterInfo)
	}()
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

		// Map to avoid launching duplicate tasks in the same batch slice
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
					log.Printf("No %s tasks available", PENDING)
				case sched.Pending.Closed(err):
					break ReadTasks
				default:
					log.Printf("Failed to read from %s queue: %s", PENDING, err)
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

func (sched *Scheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	taskId := status.TaskId.GetValue()
	taskStatus := status.GetState()
	log.Println("Task", taskId, "is in state", taskStatus.String())

	switch taskStatus {
	case mesos.TaskState_TASK_RUNNING:
		log.Printf("Marking task %s as %s", taskId, RUNNING)
	case mesos.TaskState_TASK_KILLED, mesos.TaskState_TASK_FINISHED, mesos.TaskState_TASK_FAILED, mesos.TaskState_TASK_LOST:
		log.Printf("Marking task %s as %s", taskId, STOPPED)
	}
}

func (sched *Scheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

func (sched *Scheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

func (sched *Scheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}

func (sched *Scheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {}

func (sched *Scheduler) Error(driver sched.SchedulerDriver, err string) {
	sched.errChan <- fmt.Errorf("cheduler received error: %s", err)
}
