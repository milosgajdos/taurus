package taurus

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

type JobWorker interface {
	// Run starts Worker and blocks until either it is stopped or
	// any of the by Worker started goroutines fail with error
	// Worker reads the Jobs from store, queues them for scheduling
	// and kills the Jobs when Job is requested to be stopped
	Start(scheduler.SchedulerDriver, *mesos.MasterInfo) error
	// HandleResouceOffers reads in the offers from Mesos master
	// and attempts to launch the tasks from the Pending task queue
	Schedule(scheduler.SchedulerDriver, []*mesos.Offer)
	// Stop stops the Worker and all the groutines started by it
	// Stopping Worker waits for all Worker goroutines to be stopped.
	Stop()
}
