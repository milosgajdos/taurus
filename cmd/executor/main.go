package main

import (
	"log"

	executor "github.com/mesos/mesos-go/executor"
	"github.com/milosgajdos83/taurus"
)

func main() {
	log.Println("Starting Taurus Executor")

	config := executor.DriverConfig{
		Executor: taurus.NewExecutor(),
	}

	driver, err := executor.NewMesosExecutorDriver(config)
	if err != nil {
		log.Fatal("Unable to create a ExecutorDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		log.Printf("Taurus Executor stopped with status %s and error %s\n", stat.String(), err.Error())
	}
}
