package taurus

import "fmt"

const (
	ErrFailedRead ErrorCode = iota + 1
	ErrFailedWrite
	ErrFailedDelete
	ErrExists
	ErrNotFound
)

// Store is a generic Job store
type Store interface {
	// AddJob adds a new Job to the store
	AddJob(*Job) error
	// RemoveJob removes an existing Job from the store
	RemoveJob(string) error
	// UpdateJob updates an existing Job in the store
	UpdateJob(*Job) error
	// GetJob retrieves an existing Job from the store
	GetJob(string) (*Job, error)
	// GetJobs retrieves all Jobs in a given state
	GetJobs(State) ([]*Job, error)
	// GetAllJobs retrieves all Jobs from the store
	GetAllJobs() ([]*Job, error)
	// Close closes open store and its underlying file descriptors
	Close()
}

// ErrorCode defines Store operation error code
// ErrorCode implements fmt.Stringer interface
type ErrorCode int

// String method implementation to satisfy fmt.Stringer interface
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

// StoreError encapsulates ErrorCode and adds a simple error description
// StoreError implements builtin error interface
type StoreError struct {
	Code ErrorCode
	Err  error
}

// Error interface implementation to satisfy builtin error interface
func (se *StoreError) Error() string {
	s := se.Code.String()
	if se.Err != nil {
		s += se.Err.Error()
	}
	return fmt.Sprintf("[Store Error] ", s)
}
