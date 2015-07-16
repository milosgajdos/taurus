package taurus

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
	"unsafe"

	"code.google.com/p/go-uuid/uuid"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

var native binary.ByteOrder

func init() {
	var x uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&x)) == 0x01 {
		native = binary.BigEndian
	} else {
		native = binary.LittleEndian
	}
}

func ParseAddr(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	addrParts := strings.SplitN(addr, "://", 2)
	if len(addrParts) == 1 {
		return addrParts[0], nil
	}

	if addrParts[1] == "" {
		return "", fmt.Errorf("Missing bind address info: %s", addr)
	}

	return addrParts[1], nil
}

func ParseJobId(taskId string) string {
	idSlice := strings.Split(taskId, "-")
	if len(idSlice) < 2 {
		return ""
	}
	return idSlice[0]
}

func MasterConnStr(masterInfo *mesos.MasterInfo) string {
	ip := make([]byte, 4)
	native.PutUint32(ip, masterInfo.GetIp())
	addr := net.IP(ip).To4().String()
	port := masterInfo.GetPort()
	return fmt.Sprintf("%s:%d", addr, port)
}

func MesosTasks(ctx context.Context, master, jobId string, state *mesos.TaskState) (map[string]string, error) {
	uri := fmt.Sprintf("http://%s/master/state.json", master)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	mesosTasks := make(map[string]string)
	err = httpDo(ctx, req, func(res *http.Response, err error) error {
		if err != nil {
			return err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			return fmt.Errorf("HTTP request failed with code %d: %v", res.StatusCode, res.Status)
		}

		data := struct {
			Frameworks []struct {
				Name  string `json:"name"`
				Tasks []struct {
					Id      string `json:"id"`
					State   string `json:"state"`
					SlaveId string `json"slave_id"`
				} `json:"tasks"`
			} `json:"frameworks"`
		}{}

		if err = json.NewDecoder(res.Body).Decode(&data); err != nil {
			return err
		}

		for _, fw := range data.Frameworks {
			if fw.Name != FrameworkName {
				continue
			}
			for _, task := range fw.Tasks {
				if jobId == "" || ParseJobId(task.Id) == jobId {
					if state != nil {
						if task.State == state.String() {
							mesosTasks[task.Id] = task.SlaveId
						}
					} else {
						mesosTasks[task.Id] = task.SlaveId
					}
				}
			}
		}
		return nil
	})
	return mesosTasks, err
}

// Shamelessly stolen from https://github.com/mesos/mesos-go/blob/master/detector/standalone.go#L232
type responseHandler func(*http.Response, error) error

func httpDo(ctx context.Context, req *http.Request, f responseHandler) error {
	tr := &http.Transport{}
	client := &http.Client{
		Transport: tr,
	}
	ch := make(chan error, 1)
	go func() { ch <- f(client.Do(req)) }()
	select {
	case <-ctx.Done():
		tr.CancelRequest(req)
		<-ch
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func ScalarResourceVal(name string, resources []*mesos.Resource) float64 {
	scalarResources := util.FilterResources(resources, func(res *mesos.Resource) bool {
		return res.GetType() == mesos.Value_SCALAR && res.GetName() == name
	})
	sum := 0.0
	for _, res := range scalarResources {
		sum += res.GetScalar().GetValue()
	}
	return sum
}

func CreateMesosTaskInfo(jobId string, task *JobTask) *mesos.TaskInfo {
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
