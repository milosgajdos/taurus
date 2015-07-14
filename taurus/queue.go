package taurus

import "time"

type Subscription interface {
	AutoUnsubscribe(int) error
	NextTask(time.Duration) (*Task, error)
	TimedOut(error) bool
	ConnClosed(error) bool
	Unsubscribe() error
}

type Queue interface {
	Publish(string, interface{}) error
	Subscribe(string) (Subscription, error)
	Close()
}
