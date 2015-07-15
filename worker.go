package taurus

import "github.com/mesos/mesos-go/scheduler"

type TaskWorker interface {
	Run(*scheduler.MesosSchedulerDriver) <-chan error
	KillDoomedTasks(*scheduler.MesosSchedulerDriver) error
	GeneratePendingTasks() error
	QueuePendingTasks() error
	QueueDoomedTasks() error
	CleanDoomedTasks() error
	ReconcilePendingTasks() error
	ReconcileDoomedTasks() error
	Stop()
}
