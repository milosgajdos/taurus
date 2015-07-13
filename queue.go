package taurus

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats"
)

type Queue interface {
	Publish(string, interface{}) error
	Subscribe(string) (Subscription, error)
	Close()
}

type Subscription interface {
	AutoUnsubscribe(int) error
	IsValid() bool
	NextTask(time.Duration) (*Task, error)
	TimedOut(error) bool
	ConnClosed(error) bool
	Unsubscribe() error
}

type TaskQueue struct {
	enconn *nats.EncodedConn
}

func NewTaskQueue(options *nats.Options, encType string) (*TaskQueue, error) {
	conn, err := options.Connect()
	if err != nil {
		return nil, err
	}

	enconn, err := nats.NewEncodedConn(conn, encType)
	if err != nil {
		return nil, err
	}

	return &TaskQueue{
		enconn: enconn,
	}, nil
}

func (tq *TaskQueue) Publish(subject string, data interface{}) error {
	return tq.enconn.Publish(subject, data)
}

func (tq *TaskQueue) Subscribe(subject string) (Subscription, error) {
	sub, err := tq.enconn.Conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}
	return &TaskSubscription{
		sub: sub,
	}, nil
}

func (tq *TaskQueue) Close() {
	tq.enconn.Close()
}

func (tq *TaskQueue) Conn() *nats.Conn {
	return tq.enconn.Conn
}

type TaskSubscription struct {
	sub *nats.Subscription
}

func (t *TaskSubscription) AutoUnsubscribe(max int) error {
	return t.sub.AutoUnsubscribe(max)
}

func (t *TaskSubscription) IsValid() bool {
	return t.sub.IsValid()
}

func (t *TaskSubscription) NextTask(timeout time.Duration) (*Task, error) {
	m, err := t.sub.NextMsg(timeout)
	if err != nil {
		return nil, err
	}
	task := new(Task)
	if err := json.Unmarshal(m.Data, task); err != nil {
		return nil, err
	}
	return task, nil
}

func (t *TaskSubscription) TimedOut(err error) bool {
	if err == nats.ErrTimeout {
		return true
	}
	return false
}

func (t *TaskSubscription) ConnClosed(err error) bool {
	if err == nats.ErrConnectionClosed {
		return true
	}
	return false
}

func (t *TaskSubscription) Unsubscribe() error {
	return t.sub.Unsubscribe()
}
