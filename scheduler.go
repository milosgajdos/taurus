package taurus

import (
	"fmt"
	"log"
	"sync"

	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

// Scheduler is Taurus Framework Mesos scheduler
type Scheduler struct {
	Worker  Worker
	done    chan struct{}
	errChan chan error
	wg      sync.WaitGroup
}

// Initializes Framework Scheduler
// It returns pointer to Scheduler and error.
// Eror is reserved for future use here. At the moment this method will never return error
func NewScheduler(worker Worker) (*Scheduler, error) {
	errChan := make(chan error, 2)
	done := make(chan struct{})

	return &Scheduler{
		Worker:  worker,
		done:    done,
		errChan: errChan,
	}, nil
}

// Run starts waiting for any of the Scheduler work goroutines to return error
// It would be more appropriate to call this method Wait.
// Scheduler worker is started as soon as the framework is registered with Mesos
func (sched *Scheduler) Run(driver sched.SchedulerDriver) error {
	var err error
	sched.wg.Add(1)
	go func() {
		log.Printf("Starting %s scheduler driver", FrameworkName)
		defer sched.wg.Done()
		if status, err := driver.Run(); err != nil {
			sched.errChan <- fmt.Errorf("Driver failed to start with status %s: %s",
				status.String(), err)
		}
	}()

	select {
	case <-sched.done:
		log.Printf("Stopping %s Scheduler", FrameworkName)
	case err = <-sched.errChan:
		log.Printf("Stopping %s Scheduler due to error: %s", FrameworkName, err)
	}

	return err
}

// Stop stops Scheduler and all of its worker goroutines
func (sched *Scheduler) Stop() {
	close(sched.done)
	sched.Worker.Stop()
	if _, err := driver.Stop(false); err != nil {
		log.Printf("Stopping %s scheduler driver failed: %s", FrameworkName, err)
	}
	sched.wg.Wait()
	return
}

func (sched *Scheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Println(FrameworkName, "Framework Registered with Master", MasterConnStr(masterInfo))
	sched.wg.Add(1)
	go func() {
		defer sched.wg.Done()
		var wg sync.WaitGroup
		errChan := make(chan error)
		wg.Add(1)
		go func() {
			log.Printf("Starting %s framework scheduler worker", FrameworkName)
			errChan <- sched.Worker.Start(driver, masterInfo)
		}()
		select {
		case <-sched.done:
			log.Printf("Stopping %s Worker", FrameworkName)
		case errChan <- err:
			log.Printf("Stopping %s worker due to error: %s", FrameworkName, err)
		}
		wg.Wait()
		log.Printf("%s scheduler worker stopped", FrameworkName)
	}()
}

func (sched *Scheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	//TODO: reconcile the Job Tasks here
	// Mark all the RUNNING jobs as PENDING and let Worker take care of the rest
	log.Printf("Stopping %s framework scheduler worker", FrameworkName)
	sched.Worker.Stop()
	log.Println(FrameworkName, "Framework Re-Registered with Master", MasterConnStr(masterInfo))
	go func() {
		log.Printf("Starting %s framework scheduler worker", FrameworkName)
		sched.errChan <- sched.Worker.Start(driver, masterInfo)
		log.Printf("%s scheduler worker stopped", FrameworkName)
	}()
}

func (sched *Scheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("%s Scheduler Disconnected from Master", FrameworkName)
}

func (sched *Scheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	sched.Worker.ScheduleTasks(driver, offers)
}

func (sched *Scheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	sched.Worker.StatusUpdate(driver, status)
}

func (sched *Scheduler) OfferRescinded(driver sched.SchedulerDriver, offerId *mesos.OfferID) {
	log.Printf("Offer %s no longer valid", offerId.GetValue())
}

func (sched *Scheduler) Error(driver sched.SchedulerDriver, err string) {
	sched.errChan <- fmt.Errorf("cheduler received error: %s", err)
}

func (sched *Scheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

func (sched *Scheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {
	// TODO: reconcile tasks running on lost slave
}

func (sched *Scheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
	// TODO: reconcile tasks when Docker daemon stops/crashes
}
