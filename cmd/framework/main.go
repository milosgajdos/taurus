package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/milosgajdos83/taurus"
	"github.com/milosgajdos83/taurus/queue"
	"github.com/milosgajdos83/taurus/store"
	"github.com/milosgajdos83/taurus/worker"
)

var (
	master    = flag.String("master", "localhost:5050", "Mesos master to register with")
	listen    = flag.String("listen", ":8080", "API listen address")
	taskqueue = flag.String("taskqueue", "", "Task Queue URL")
	user      = flag.String("user", "", "User to execute tasks as")
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func ParseCli() (string, string, []string) {
	flag.Parse()
	mesosMaster := os.Getenv("MESOS_MASTER")
	if mesosMaster == "" {
		mesosMaster = *master
	}
	masterAddr, err := taurus.ParseAddr(mesosMaster)
	if err != nil {
		log.Fatal(err)
	}

	taurusListen := os.Getenv("TAURUS_LISTEN")
	if taurusListen == "" {
		taurusListen = *listen
	}
	listenAddr, err := taurus.ParseAddr(taurusListen)
	if err != nil {
		log.Fatal(err)
	}

	taskQueue := strings.Split(os.Getenv("TASK_QUEUE"), ",")
	if len(taskQueue) == 1 && taskQueue[0] == "" {
		taskQueue = []string{*taskqueue}
	}

	return masterAddr, listenAddr, taskQueue
}

func main() {
	mesosMaster, listenAddr, queueServers := ParseCli()
	ts, err := store.NewBasicStore("/tmp/taurus.db")
	if err != nil {
		log.Fatal(err)
	}

	options := queue.DefaultOptions
	queueURL := queue.DefaultURL
	if len(queueServers) == 1 && queueServers[0] == "" {
		queueServers = append(queueServers, queueURL)
	}
	options.Servers = queueServers
	encoder = queue.DefaultEncoder
	tq, err := queue.NewBasicQueue(&options, encoder)
	if err != nil {
		log.Fatal(err)
	}

	tw, err := worker.NewBasicWorker(ts, tq)
	if err != nil {
		log.Fatal(err)
	}

	t, err := taurus.NewFramework(&taurus.Config{
		Master:     mesosMaster,
		ListenAddr: listenAddr,
		User:       *user,
		Store:      ts,
		Queue:      tq,
		Worker:     tw,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := t.Run(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
