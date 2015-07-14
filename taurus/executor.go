package taurus

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (exec *Executor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *Executor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *Executor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (exec *Executor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())
	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}

	//
	// TODO: IMPLEMENT THE ACTUAL WORK HERE
	//

	fmt.Println("Finishing task", taskInfo.GetName())
	finStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_FINISHED.Enum(),
	}
	_, err = driver.SendStatusUpdate(finStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}
	fmt.Println("Task finished", taskInfo.GetName())
}

func (exec *Executor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (exec *Executor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (exec *Executor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (exec *Executor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func Info() *mesos.ExecutorInfo {
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("taurus"),
		Name:       proto.String("Taurus Executor"),
		Source:     proto.String("taurus_executor"),
		Command: &mesos.CommandInfo{
			Shell: proto.Bool(false),
		},
	}
}
