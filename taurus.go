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

// Config allows to specify Taurus Framework configuration
type Config struct {
	// Mesos master
	Master string
	// Framework API Listen Address
	ListenAddr string
	// Unix user the tasks should be launched as
	User string
	// Framework Job store
	Store Store
	// Framework Task Queue
	Queue TaskQueue
	// Framework Scheduler Worker
	Worker Worker
	// TLS configuration
	TlsConfig *tls.Config
}

// Taurus is a Mesos Framework with simple REST API
type Taurus struct {
	fwInfo    *mesos.FrameworkInfo
	driver    *scheduler.MesosSchedulerDriver
	scheduler *Scheduler
	api       *Api
}

// NewFramework initializes Taurus framework with the provided configuration options
//
// NewFramework initializes Mesos Scheduler driver and creates HTTP API server
// It returns error if either Mesos driver or HTTP API server could not be created
func NewFramework(config *Config) (*Taurus, error) {
	fwInfo := &mesos.FrameworkInfo{
		User: proto.String(config.User),
		Name: proto.String(FrameworkName),
	}

	sched, err := NewScheduler(config.Worker)
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
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create %s API server: %s", FrameworkName, err)
	}

	return &Taurus{
		fwInfo:    fwInfo,
		driver:    driver,
		scheduler: sched,
		api:       api,
	}, nil
}

// Run starts Taurus framework
//
// Run launches Mesos Mesos task scheduler and framework's API server in separate goroutines
// Run blocks until either of the launched goroutines fails with error or if the framework has been manually stopped by sending it either of SIGTERM, SIGKILL or SIGINT OS signals
// Run waits for all goroutines to finish cleanly and returns error back to the caller
func (t *Taurus) Run() error {
	var err error
	var wg sync.WaitGroup

	// Create error channel
	errChan := make(chan error, 2)

	// Signal handler to stop the framework scheduler and API
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)

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
		log.Printf("%s shutting down. Got signal: %s", FrameworkName, sig)
	case err = <-errChan:
		log.Printf("%s Framework failed with error: %s", FrameworkName, err)
	}

	log.Printf("Stopping %s API server", FrameworkName)
	t.api.listener.Close()
	log.Printf("Stopping %s Scheduler", FrameworkName)
	t.scheduler.Stop()
	wg.Wait()

	return err
}
