package queue

import (
	"encoding/json"
	"time"

	"github.com/milosgajdos83/taurus"
	"github.com/nats-io/nats"
)

const (
	DefaultURL     = nats.DefaultURL
	DefaultEncoder = nats.JSON_ENCODER
)

var DefaultOptions = &nats.DefaultOptions

type BasicQueue struct {
	enconn *nats.EncodedConn
}

func NewBasicQueue(options *nats.Options, encType string) (*BasicQueue, error) {
	conn, err := options.Connect()
	if err != nil {
		return nil, err
	}

	enconn, err := nats.NewEncodedConn(conn, encType)
	if err != nil {
		return nil, err
	}

	return &BasicQueue{
		enconn: enconn,
	}, nil
}

func (bq *BasicQueue) Publish(subject string, data interface{}) error {
	return bq.enconn.Publish(subject, data)
}

func (bq *BasicQueue) Subscribe(subject string) (taurus.Subscription, error) {
	sub, err := bq.enconn.Conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}
	return &TaskSubscription{
		sub: sub,
	}, nil
}

func (bq *BasicQueue) Close() {
	bq.enconn.Close()
}

func (bq *BasicQueue) Conn() *nats.Conn {
	return bq.enconn.Conn
}

type TaskSubscription struct {
	sub *nats.Subscription
}

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

func (ts *TaskSubscription) AutoUnsubscribe(max int) error {
	return ts.sub.AutoUnsubscribe(max)
}

func (ts *TaskSubscription) Unsubscribe() error {
	return ts.sub.Unsubscribe()
}

func (ts *TaskSubscription) TimedOut(err error) bool {
	if err == nats.ErrTimeout {
		return true
	}
	return false
}

func (ts *TaskSubscription) Closed(err error) bool {
	if err == nats.ErrConnectionClosed {
		return true
	}
	return false
}
