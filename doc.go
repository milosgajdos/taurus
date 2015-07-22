// Package taurus allows to create simple Mesos frameworks that run Docker containers
//
// You can use taurus to create your own Mesos framework and plug it into your existing Mesos setup.
// taurus leverages mesos-go API package and implements scheduler.SchedulerDriver interface
// taurus also provides a simple REST API to submit and stop Jobs
//
// You can plug in your own Job store implementation as long as it satisfies taurus.Store interface
// You can plug in your own Worker implementation as long as it satisfies taurus.Worker interface
// You can plug in your own Queue implementation as long as it satisfies taurus.TaskQueue interface
//
// A concrete implementation of sample Job store can be found in store/basicstore.go
// A concrete implementation of sample framework Worker can be found in worker/basicworker.go
// A concrete implementation of sample Task queue can be found in queue/basicqueue.go
//
// A sample framework implementation which uses taurus package can be found in cmd/framework/main.go
package taurus
