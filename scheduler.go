package taurus

import (
	"fmt"
	"log"

	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

type Scheduler struct {
	Worker  Worker
	master  string
	errChan chan error
}

func NewScheduler(worker Worker, master string) (*Scheduler, error) {
	errChan := make(chan error)

	return &Scheduler{
		Worker:  worker,
		master:  master,
		errChan: errChan,
	}, nil
}

func (sched *Scheduler) Run(driver *sched.MesosSchedulerDriver) (err error) {
	return <-sched.errChan
}

func (sched *Scheduler) Stop() {
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
		sched.errChan <- sched.Worker.Start(driver, masterInfo)
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
		sched.errChan <- sched.Worker.Start(driver, masterInfo)
	}()
}

func (sched *Scheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Taurus Scheduler Disconnected from Master")
}

func (sched *Scheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	sched.Worker.ScheduleTasks(driver, offers)
}

func (sched *Scheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	sched.Worker.StatusUpdate(driver, status)
}

func (sched *Scheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

func (sched *Scheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

func (sched *Scheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}

func (sched *Scheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {}

func (sched *Scheduler) Error(driver sched.SchedulerDriver, err string) {
	sched.errChan <- fmt.Errorf("cheduler received error: %s", err)
}
