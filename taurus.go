package taurus

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	scheduler "github.com/mesos/mesos-go/scheduler"
)

const (
	FrameworkName = "taurus"
)

type Config struct {
	// Mesos master
	Master string
	//Taurus API Listen Address
	ListenAddr string
	// User task runner
	User string
	// Framework store
	Store Store
	// Task Queue
	Queue Queue
	// TLS configuration
	TlsConfig *tls.Config
}

type Taurus struct {
	fwInfo    *mesos.FrameworkInfo
	driver    *scheduler.MesosSchedulerDriver
	scheduler *Scheduler
	api       *Api
}

func NewFramework(config *Config) (*Taurus, error) {
	fwInfo := &mesos.FrameworkInfo{
		User: proto.String(config.User),
		Name: proto.String(FrameworkName),
	}

	sched, err := NewScheduler(config.Store, config.Queue, config.Master)
	if err != nil {
		return nil, fmt.Errorf("Unable to create %s Scheduler: %s", FrameworkName, err)
	}
	driverConfig := scheduler.DriverConfig{
		Scheduler: sched,
		Framework: fwInfo,
		Master:    config.Master,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(driverConfig)
	if err != nil {
		return nil, fmt.Errorf("Unable to create a SchedulerDriver: %s", err)
	}

	api, err := NewApi(&ApiConfig{
		Address:   config.ListenAddr,
		TlsConfig: config.TlsConfig,
		Store:     config.Store,
		Master:    config.Master,
	})
	if err != nil {
		return nil, fmt.Errorf("Could not start API server: %s", err)
	}

	return &Taurus{
		fwInfo:    fwInfo,
		driver:    driver,
		scheduler: sched,
		api:       api,
	}, nil
}

func (t *Taurus) Run() error {
	var err error
	var wg sync.WaitGroup

	// Create error channel
	errChan := make(chan error, 3)

	// Signal handler to stop API, Scanner and Killer goroutines
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)

	// Start Taurus Scheduler driver
	go func() {
		if st, err := t.driver.Run(); err != nil {
			errChan <- fmt.Errorf("Taurus failed with status %s: %s", st.String(), err)
		}
	}()

	// Start Scheduler
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Starting %s Scheduler", FrameworkName)
		errChan <- t.scheduler.Run(t.driver)
	}()

	// Start Taurus API
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Starting %s API server", FrameworkName)
		errChan <- t.api.ListenAndServe()
	}()

	select {
	case sig := <-sigc:
		log.Printf("Taurus shutting down. Got signal: %s", sig)
	case err = <-errChan:
		log.Printf("Taurus failed with error: %s", err)
	}

	log.Printf("Shutting down %s API server", FrameworkName)
	t.api.listener.Close()
	log.Printf("Stopping %s Scheduler", FrameworkName)
	t.scheduler.Stop()
	wg.Wait()
	log.Printf("Stopping %s Scheduler driver", FrameworkName)
	if _, err := t.driver.Stop(false); err != nil {
		log.Printf("Stopping %s scheduler driver failed: %s", err, FrameworkName)
	}

	return err
}
