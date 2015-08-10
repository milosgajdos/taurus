package taurus

import "time"

// Subscription is a TaskQueue topic subscription
type Subscription interface {
	// ReadTask waits for some time to read Task from TaskQueue
	ReadTask(time.Duration) (*Task, error)
	// AutoUnsubscribe automatically unsubscribes from Subscription once some
	// number of messages have been received
	AutoUnsubscribe(int) error
	// Unsubscribe unsubscribes from Subscription
	Unsubscribe() error
	// TimedOut checks if the error returned from Subscription read means TaskQueue timed out
	TimedOut(error) bool
	// Closed checks if the error returned from Subscription read means TaskQueue is Closed
	Closed(error) bool
}

// TaskQueue is a generic queue for taurus.Tasks
type TaskQueue interface {
	// Connect establishes a connection with TaskQueue
	Connect()
	// Publish publishes a Task to TaskQueue
	Publish(string, *Task) error
	// Subscribe subscribes to a topic and returns Subscription
	Subscribe(string) (Subscription, error)
	// Close closes the TaskQeue
	Close()
}
