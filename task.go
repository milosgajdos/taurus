package taurus

import (
	"fmt"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type State int

const (
	// Taurus Job state
	Pending State = iota + 1
	Scheduling
	Running
	Doomed
	Killing
	Dead
	Unknown
	// Default Task resource allocations
	DEFAULT_CPUS_PER_TASK = 1
	DEFAULT_MEM_PER_TASK  = 128
)

func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Scheduling:
		return "Scheduling"
	case Running:
		return "Running"
	case Doomed:
		return "Doomed"
	case Killing:
		return "Killing"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// Volume defines container volume-host mapping
type Volume struct {
	ContainerPath string `json:"container_path"`
	HostPath      string `json:"host_path"`
	Mode          string `json:"mode"`
}

// Container is any container supported by Mesos
type Container struct {
	Command []string  `json:"command"`
	Image   string    `json:"image"`
	Volumes []*Volume `json:"volumes"`
}

// Resources represents compute resource to allocate to each Task replica
type Resources struct {
	Cpu    float64 `json:"cpu"`
	Memory float64 `json:"memory"`
}

// HealthCheck allows to specity HTTP healthcheck for a Task
type HealthCheck struct {
	Port     uint32  `json:"port"`
	Path     string  `json:"path"`
	Interval float64 `json:"interval"`
	Timeout  float64 `json:"timeout"`
	Failures uint32  `json:"failures"`
}

// JobTask defines Taurus Job Task template
type JobTask struct {
	Cluster     string       `json:"cluster"`
	Role        string       `json:"role"`
	Environment string       `json:"environment"`
	Priority    uint32       `json:"priority"`
	Container   *Container   `json:"container"`
	Resources   *Resources   `json:"resources"`
	HealthCheck *HealthCheck `json:"health_check"`
	Replicas    uint32       `json:"replicas"`
}

// Job is a Taurus Framework Job
// It can schedule multiple replicas of JobTask
type Job struct {
	Id    string   `json:"id"`
	Task  *JobTask `json:"task"`
	State State    `json:"job_state"`
}

// Task is an instance of JobTask
type Task struct {
	Info  *mesos.TaskInfo `json:"info"`
	JobId string          `json:"job_id"`
}

func createMesosTaskInfo(jobId string, task *JobTask) *mesos.TaskInfo {
	// TODO: Needs proper randomization
	taskIdValue := fmt.Sprintf("%s-%s-%s-%d-%s",
		jobId,
		task.Role,
		task.Environment,
		time.Now().Unix(),
		uuid.NewUUID())

	taskId := &mesos.TaskID{
		Value: proto.String(taskIdValue),
	}

	// Define ContainerInfo
	container := &mesos.ContainerInfo{
		Type: mesos.ContainerInfo_DOCKER.Enum(),
		Docker: &mesos.ContainerInfo_DockerInfo{
			Image:   proto.String(task.Container.Image),
			Network: mesos.ContainerInfo_DockerInfo_BRIDGE.Enum(),
		},
	}

	// Container Volumes
	for _, v := range task.Container.Volumes {
		var (
			vv   = v
			mode = mesos.Volume_RW.Enum()
		)

		if vv.Mode == "ro" {
			mode = mesos.Volume_RO.Enum()
		}

		container.Volumes = append(container.Volumes, &mesos.Volume{
			ContainerPath: &vv.ContainerPath,
			HostPath:      &vv.HostPath,
			Mode:          mode,
		})
	}

	// Set Task resources
	if task.Resources != nil {
		if task.Resources.Cpu == 0.0 {
			task.Resources.Cpu = DEFAULT_CPUS_PER_TASK
		}

		if task.Resources.Cpu == 0.0 {
			task.Resources.Memory = DEFAULT_MEM_PER_TASK
		}
	} else {
		task.Resources = &Resources{
			Cpu:    DEFAULT_CPUS_PER_TASK,
			Memory: DEFAULT_MEM_PER_TASK,
		}
	}

	// *mesos.TaskInfo
	taskInfo := &mesos.TaskInfo{
		Name:   proto.String(fmt.Sprintf("taurus-task-%s", taskIdValue)),
		TaskId: taskId,
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", task.Resources.Cpu),
			util.NewScalarResource("mem", task.Resources.Memory),
		},
		Command: &mesos.CommandInfo{
			Shell: proto.Bool(false),
		},
		Container: container,
		Data:      []byte(jobId),
	}

	//Set value only if provided
	if len(task.Container.Command) == 1 {
		if task.Container.Command[0] != "" {
			taskInfo.Command.Value = &task.Container.Command[0]
		}
	}
	// Set args only if they exist
	if len(task.Container.Command) > 1 {
		taskInfo.Command.Arguments = task.Container.Command[1:]
	}

	return taskInfo
}
