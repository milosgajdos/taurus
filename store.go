package taurus

import "fmt"

type Store interface {
	// Add a New Taurus Job
	AddJob(*Job) error
	// Removes a Taurus Job
	RemoveJob(string) error
	// Retrieve a Taurus job from Job catalog
	GetJob(string) (*Job, error)
	// Retrieve all Taurus Jobs in a given Job state
	GetJobs(State) ([]*Job, error)
	// UpdateJob Taurus Job in Job catalog
	UpdateJob(*Job) error
	// Retrieve all Taurus Jobs from Job catalog
	GetAllJobs() ([]*Job, error)
	// AddTask() temporarily stores
	AddTask(*Task) error
	// RemoveTask() removes scheduled tasks
	RemoveTask(string) error
	// GetTask(string) returns task
	GetTask(string) (*Task, error)
	// GetTasks() returns a list of tasks for given Job
	GetTasks(State) ([]*Task, error)
	// UpdateTask() updates the stored task
	UpdateTask(*Task) error
	// GetTasks() returns a list of tasks for given Job
	GetJobTasks(string) ([]*Task, error)
}

type ErrorCode int

const (
	ErrFailedRead ErrorCode = iota + 1
	ErrFailedWrite
	ErrFailedDelete
	ErrExists
	ErrNotFound
)

func (ec ErrorCode) String() string {
	switch ec {
	case ErrFailedRead:
		return "Failed to read item"
	case ErrFailedWrite:
		return "Failed to write item"
	case ErrFailedDelete:
		return "Failed to delete item"
	case ErrExists:
		return "Item already exists"
	case ErrNotFound:
		return "Item not found"
	default:
		return "Unknown"
	}
}

type StoreError struct {
	Code ErrorCode
	Err  error
}

func (se *StoreError) Error() string {
	s := se.Code.String()
	if se.Err != nil {
		s += se.Err.Error()
	}
	return fmt.Sprintf("[Store Error] ", s)
}
