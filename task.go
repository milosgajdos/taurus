package taurus

import mesos "github.com/mesos/mesos-go/mesosproto"

type State int

const (
	// Taurus Job state
	Pending State = iota + 1
	Scheduled
	Running
	Doomed
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
	case Scheduled:
		return "Scheduled"
	case Running:
		return "Running"
	case Doomed:
		return "Doomed"
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
	State State           `json:"task_state"`
}
