package queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/milosgajdos83/taurus"
	"github.com/nats-io/nats"
)

const (
	DefaultURL     = nats.DefaultURL
	DefaultEncoder = nats.JSON_ENCODER
)

var DefaultOptions = nats.DefaultOptions

// BasicQueue provides a basic implementation of taurus.TaskQueue interface
// BasicQueue uses nats.io distributed queue for data transport
type BasicQueue struct {
	enconn  *nats.EncodedConn
	Options *nats.Options
	encoder string
}

// NewBasicQueue initializes BasicQueue client
//
// NewBasicQueue accepts nats.Options and encoder type.
// If no options are provided, BasicQueue will be initialized with DefaultOptions
// It returns error if the queue fails to be initialized.
func NewBasicQueue(options *nats.Options, encoder string) (*BasicQueue, error) {
	if options == nil {
		options := &DefaultOptions
	}

	if encoder != nats.JSON_ENCODER || nats.GOB_ENCODER {
		return nil, fmt.Errorf("Unsupported encoder type: %s", encoder)
	}

	return &BasicQueue{
		encoder: encoder,
		Options: options,
	}, nil
}

// Connect establishes encoded connection with Queue
// Connect returns error if either the connection could not be established or
// requested Encoder scheme is not supported
func (bq *BasicQueue) Connect() error {
	conn, err := bq.Options.Connect()
	if err != nil {
		return err
	}

	enconn, err := nats.NewEncodedConn(conn, bq.encoder)
	if err != nil {
		return err
	}
	bq.enconn = enconn

	return nil
}

// Publish enqueues a taurus Task to BasicQueue
// It returns an error if publishing the data to queue failed.
func (bq *BasicQueue) Publish(subject string, task *taurus.Task) error {
	return bq.enconn.Publish(subject, task)
}

// Subscribe creates a Synchronous subscription to given subject.
// It returns error if the subscribing fails. This can be due to closed queue.
func (bq *BasicQueue) Subscribe(subject string) (taurus.Subscription, error) {
	sub, err := bq.enconn.Conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}
	return &TaskSubscription{
		sub: sub,
	}, nil
}

// Close closes the Queue connection
// All subsequent Subscribe and Publish calls will fail from now on with nats.ErrConnectionClosed error
func (bq *BasicQueue) Close() {
	bq.enconn.Close()
}

// Conn returns a raw nats queue connection
func (bq *BasicQueue) Conn() *nats.Conn {
	return bq.enconn.Conn
}

// TaskSubscription provides a generic implementation of taurus.Subscription interface
// On low level it wraps *nats.Subscription and provides a simple API to it
type TaskSubscription struct {
	sub *nats.Subscription
}

// ReadTask waits for data to arrive on a given Subscription topic for a provided timeout time
//
// ReadTask umarshals the raw data based on encodeing type into taurus.Task and returns it
// It returns error if either unmarshaling data failed, queue has been closed or a time out occurred
func (ts *TaskSubscription) ReadTask(timeout time.Duration) (*taurus.Task, error) {
	m, err := ts.sub.NextMsg(timeout)
	if err != nil {
		return nil, err
	}
	task := new(taurus.Task)
	if err := json.Unmarshal(m.Data, task); err != nil {
		return nil, err
	}
	return task, nil
}

// AutoUnsubscribe automatically unsubscribes client from Subscription once max messages have been received
func (ts *TaskSubscription) AutoUnsubscribe(max int) error {
	return ts.sub.AutoUnsubscribe(max)
}

// Unsubscribe unsubscribes client from subscription
func (ts *TaskSubscription) Unsubscribe() error {
	return ts.sub.Unsubscribe()
}

// TimedOut inspects the error passed in as a parameter and returns true or false based on the type of error
// If the passed in error is nats.ErrTimeout it returns true. Otherwise it returns false
func (ts *TaskSubscription) TimedOut(err error) bool {
	if err == nats.ErrTimeout {
		return true
	}
	return false
}

// Closed inspects the error passed in as a parameter and returns true or false based on the type of error
// If the passed in error is nats.ErrConnectionClosed it returns true. Otherwise it returns false.
func (ts *TaskSubscription) Closed(err error) bool {
	if err == nats.ErrConnectionClosed {
		return true
	}
	return false
}
