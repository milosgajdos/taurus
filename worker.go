package taurus

import "github.com/mesos/mesos-go/scheduler"

type TaskWorker interface {
	Run(scheduler.SchedulerDriver, string) error
	KillDoomedTasks(scheduler.SchedulerDriver) error
	GeneratePendingTasks() error
	QueuePendingTasks() error
	QueueDoomedTasks() error
	CleanDoomedTasks() error
	ReconcilePendingTasks() error
	ReconcileDoomedTasks() error
	Stop()
}
