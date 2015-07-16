package taurus

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

type TaskWorker interface {
	Run(scheduler.SchedulerDriver, *mesos.MasterInfo) error
	KillDoomedTasks(scheduler.SchedulerDriver) error
	GeneratePendingTasks() error
	QueuePendingTasks() error
	MarkDoomedTasks() error
	QueueDoomedTasks() error
	ReconcilePendingJobs() error
	ReconcileDoomedJobs() error
	Stop()
}
