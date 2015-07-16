package taurus

import "time"

const (
	PendingQ = "pending"
)

type Subscription interface {
	ReadTask(time.Duration) (*Task, error)
	AutoUnsubscribe(int) error
	Unsubscribe() error
	TimedOut(error) bool
	Closed(error) bool
}

type TaskQueue interface {
	Publish(string, interface{}) error
	Subscribe(string) (Subscription, error)
	Close()
}
