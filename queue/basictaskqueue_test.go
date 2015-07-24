package queue

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/milosgajdos83/taurus"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/gnatsd/test"
)

var bq *BasicQueue

// GNATS Server test options
var TestOptions = server.Options{
	Host:   "localhost",
	Port:   4222,
	NoLog:  true,
	NoSigs: true,
}

func Test_PublishSubscribe(t *testing.T) {
	testSubject := "test"
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
