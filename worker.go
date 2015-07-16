package taurus

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

type JobWorker interface {
	// Run() Starts Taurus Job worker
	// JobWorker reads the Jobs from Taurus store, queues them
	// for scheduling and kills the Job Tasks when Job is requested
	// to stop. All of this is normally done in different goroutines
	// which JobWorker manages
	Run(scheduler.SchedulerDriver, *mesos.MasterInfo) error
	// Stop() Stops Taurus Job worker
	// Stopping JobWorker involves cleanly stopping all the goroutines
	// which the JobWorker may have been started
	Stop()
}
