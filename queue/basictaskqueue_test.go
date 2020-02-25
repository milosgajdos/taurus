package queue

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/milosgajdos/taurus"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/gnatsd/test"
)

var bq *BasicQueue

var TestOptions = server.Options{
	Host:   "localhost",
	Port:   4222,
	NoLog:  true,
	NoSigs: true,
}

func Test_PublishSubscribe(t *testing.T) {
	testSubject := "test_pub_sub"
	sub, err := bq.Subscribe(testSubject)
	if err != nil {
		t.Fatal("Failed to create test subscription")
	}
	taskChan := make(chan *taurus.Task)
	go func() {
		task, err := sub.ReadTask(1 * time.Second)
		if err != nil {
			taskChan <- nil
		}
		taskChan <- task
	}()

	testTask := &taurus.Task{
		JobId: "test-job",
		State: taurus.PENDING,
	}

	if err := bq.Publish(testSubject, testTask); err != nil {
		t.Fatalf("Failed to publish Task")
	}

	readTask := <-taskChan
	if readTask != nil {
		eq := reflect.DeepEqual(testTask, readTask)
		if !eq {
			t.Fatalf("Failed to receive the test task")
		}
	} else {
		t.Fatalf("Failed to receive the test task")
	}
}

func Test_AutoUnsubscribe(t *testing.T) {
	testSubject := "test_auto_unsub"
	testTask := &taurus.Task{
		JobId: "test-job",
		State: taurus.PENDING,
	}

	max := 10
	received := 0
	total := 15
	sub, err := bq.Subscribe(testSubject)
	if err != nil {
		t.Fatal("Failed to create test subscription")
	}

	for i := 0; i < total; i++ {
		if err := bq.Publish(testSubject, testTask); err != nil {
			t.Fatalf("AutoUnsubscribe test publish message failed: %s", err)
		}
	}

	sub.AutoUnsubscribe(max)
	for {
		_, err := sub.ReadTask(1 * time.Millisecond)
		if err != nil {
			break
		}
		received += 1
	}
	if received != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
}

func Test_Unsubscribe(t *testing.T) {
	testSubject := "test_auto_unsub"
	testTask := &taurus.Task{
		JobId: "test-job",
		State: taurus.PENDING,
	}

	sub, err := bq.Subscribe(testSubject)
	if err != nil {
		t.Fatal("Failed to create test subscription")
	}

	if err := bq.Publish(testSubject, testTask); err != nil {
		t.Fatalf("Unsubscribe test publish message failed: %s", err)
	}

	sub.Unsubscribe()
	if _, err := sub.ReadTask(1 * time.Millisecond); err == nil {
		t.Fatalf("ReadTask should have returned error")
	}
}

func Test_TimedOut(t *testing.T) {
	testSubject := "test_auto_unsub"
	sub, err := bq.Subscribe(testSubject)
	if err != nil {
		t.Fatal("Failed to create test subscription")
	}

	if _, err := sub.ReadTask(1 * time.Millisecond); !sub.TimedOut(err) {
		t.Fatalf("ReadTask should have failed with Time Out error")
	}
}

func TestMain(m *testing.M) {
	var err error
	gnatsd := test.RunServer(&TestOptions)
	defer gnatsd.Shutdown()
	options := DefaultOptions
	options.Servers = []string{DefaultURL}
	if gnatsd != nil {
		bq, err = NewBasicQueue(&options, DefaultEncoder)
		if err != nil {
			panic(err)
		}
	} else {
		panic("Could not start NATS queue")
	}
	ret := m.Run()
	os.Exit(ret)
}
