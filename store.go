package taurus

import "fmt"

const (
	ErrFailedRead ErrorCode = iota + 1
	ErrFailedWrite
	ErrFailedDelete
	ErrExists
	ErrNotFound
)

type Store interface {
	// AddJob Adds a New Taurus Job to the store
	AddJob(*Job) error
	// RemoveJob Removes a Taurus Job from the store
	RemoveJob(string) error
	// GetJob Retrieve a Taurus job from the store
	GetJob(string) (*Job, error)
	// UpdateJob UpdateJob Taurus Job in the store
	UpdateJob(*Job) error
	// GetJobs Retrieve all Taurus Jobs in a given Job state
	GetJobs(State) ([]*Job, error)
	// GetAllJobs Retrieve all Taurus Jobs from the store
	GetAllJobs() ([]*Job, error)
}

type ErrorCode int

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
