package taurus

import (
	"encoding/json"
	"time"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats"
)

type Queue interface {
	Publish(string, interface{}) error
	SubscribeSync(string) (Subscription, error)
	SubscribeAsync(string, nats.MsgHandler) (Subscription, error)
	RPCDriver() graft.RPCDriver
	Close()
}

type Subscription interface {
	AutoUnsubscribe(int) error
	IsValid() bool
	NextTask(time.Duration) (*Task, error)
	Unsubscribe() error
}

type TaskQueue struct {
	conn   *nats.Conn
	enconn *nats.EncodedConn
	rpc    graft.RPCDriver
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

	rpc, err := graft.NewNatsRpc(options)
	if err != nil {
		return nil, err
	}

	return &TaskQueue{
		conn:   conn,
		enconn: enconn,
		rpc:    rpc,
	}, nil
}

func (tq *TaskQueue) Publish(subject string, data interface{}) error {
	return tq.enconn.Publish(subject, data)
}

func (tq *TaskQueue) SubscribeSync(subject string) (Subscription, error) {
	sub, err := tq.conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}
	return &TaskSubscription{
		sub: sub,
	}, nil
}

func (tq *TaskQueue) SubscribeAsync(subject string, handler nats.MsgHandler) (Subscription, error) {
	sub, err := tq.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	return &TaskSubscription{
		sub: sub,
	}, nil
}

func (tq *TaskQueue) RPCDriver() graft.RPCDriver {
	return tq.rpc
}

func (tq *TaskQueue) Close() {
	tq.conn.Close()
	tq.enconn.Close()
}

func (tq *TaskQueue) Conn() *nats.Conn {
	return tq.conn
}

func (tq *TaskQueue) EnConn() *nats.EncodedConn {
	return tq.enconn
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

func (t *TaskSubscription) Unsubscribe() error {
	return t.sub.Unsubscribe()
}
