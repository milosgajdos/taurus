package taurus

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/steveyen/gkvlite"
)

type Store interface {
	// Add a New Taurus Job to Job catalog
	AddJob(*Job) error
	// Remove a Taurus Job from Job catalog
	RemoveJob(string) error
	// Retrieve a Taurus job from Job catalog
	GetJob(string) (*Job, error)
	// Retrieve all Taurus Jobs from Job catalog
	GetAllJobs() ([]*Job, error)
	// Retrieve all Taurus Jobs in a given Job state
	GetJobs(JobState) ([]*Job, error)
	// UpdateJob Taurus Job in Job catalog
	UpdateJob(*Job) error
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
		return "ErrFailedRead"
	case ErrFailedWrite:
		return "ErrFailedWrite"
	case ErrFailedDelete:
		return "ErrFailedDelete"
	case ErrExists:
		return "ErrExists"
	case ErrNotFound:
		return "ErrNotFound"
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
	return fmt.Sprintf("[BasicStore] : %s", s)
}

type BasicStore struct {
	store *gkvlite.Store
	mu    sync.RWMutex
}

func NewBasicStore(fileName string) (*BasicStore, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	s, err := gkvlite.NewStore(file)
	if err != nil {
		return nil, err
	}

	if s.GetCollection("jobs") == nil {
		s.SetCollection("jobs", nil)
	}

	err = s.Flush()
	if err != nil {
		return nil, err
	}

	return &BasicStore{
		store: s,
	}, nil
}

func (bs *BasicStore) withColContext(muType, colName string, fn func(col *gkvlite.Collection) error) error {
	switch muType {
	case "rw":
		bs.mu.Lock()
		defer bs.mu.Unlock()
	case "r":
		bs.mu.RLock()
		defer bs.mu.RUnlock()
	}
	c := bs.store.GetCollection(colName)
	if c == nil {
		return fmt.Errorf("Collection %s does not exist", colName)
	}
	return fn(c)
}

func (bs *BasicStore) withRecordContext(id string, col *gkvlite.Collection, fn func(record []byte) error) error {
	record, err := col.Get([]byte(id))
	if err != nil {
		return &StoreError{Code: ErrFailedRead, Err: err}
	}
	return fn(record)
}

// jobExists checks if an item with a given key is already present in given collection
func colItemExists(key string, col *gkvlite.Collection) bool {
	exists := false
	col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		if key == string(i.Key) {
			exists = true
			return false
		}
		return true
	})
	return exists
}

// AddJob() stores Taurus Job as JSON blob in Taurus store
func (bs *BasicStore) AddJob(job *Job) error {
	return bs.withColContext("rw", "jobs", func(col *gkvlite.Collection) error {
		if colItemExists(job.Id, col) {
			return &StoreError{Code: ErrExists}
		}
		jobData, err := json.Marshal(job)
		if err != nil {
			return &StoreError{Code: ErrFailedWrite, Err: err}
		}
		//os.Stdout.Write(jobData)
		if err := col.Set([]byte(job.Id), jobData); err != nil {
			return &StoreError{Code: ErrFailedWrite, Err: err}
		}
		return bs.store.Flush()
	})
}

// RemoveJob() removes Taurus JOb from Taurus store
func (bs *BasicStore) RemoveJob(jobId string) error {
	return bs.withColContext("rw", "jobs", func(col *gkvlite.Collection) error {
		if colItemExists(jobId, col) {
			_, err := col.Delete([]byte(jobId))
			if err != nil {
				return &StoreError{Code: ErrFailedDelete, Err: err}
			}
			return bs.store.Flush()
		}
		return &StoreError{Code: ErrNotFound}
	})
}

// GetJob() retrieves Taurus Job from Taurus store
func (bs *BasicStore) GetJob(jobId string) (*Job, error) {
	job := new(Job)
	err := bs.withColContext("r", "jobs", func(col *gkvlite.Collection) error {
		if colItemExists(jobId, col) {
			return bs.withRecordContext(jobId, col, func(record []byte) error {
				if err := json.Unmarshal(record, job); err != nil {
					return &StoreError{Code: ErrFailedRead, Err: err}
				}
				return nil
			})
		}
		return &StoreError{Code: ErrNotFound}
	})
	return job, err
}

// Jobs() returns a slice of all Taurus Jobs
func (bs *BasicStore) GetAllJobs() ([]*Job, error) {
	jobs := make([]*Job, 0, 0)
	err := bs.withColContext("r", "jobs", func(col *gkvlite.Collection) error {
		var err error
		col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
			err = bs.withRecordContext(string(i.Key), col, func(record []byte) error {
				job := new(Job)
				if err := json.Unmarshal(record, job); err != nil {
					return &StoreError{Code: ErrFailedRead, Err: err}
				}
				jobs = append(jobs, job)
				return nil
			})

			if err != nil {
				return false
			}

			return true
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// Jobs() retrieves NEW Taurus Job from Taurus store
func (bs *BasicStore) GetJobs(state JobState) ([]*Job, error) {
	jobs := make([]*Job, 0, 0)
	err := bs.withColContext("r", "jobs", func(col *gkvlite.Collection) error {
		var err error
		col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
			err = bs.withRecordContext(string(i.Key), col, func(record []byte) error {
				job := new(Job)
				if err := json.Unmarshal(record, job); err != nil {
					return &StoreError{Code: ErrFailedRead, Err: err}
				}
				if job.State == state {
					jobs = append(jobs, job)
				}
				return nil
			})

			if err != nil {
				return false
			}

			return true
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// UpdateJob() updates an existing Taurus Job
func (bs *BasicStore) UpdateJob(job *Job) error {
	return bs.withColContext("rw", "jobs", func(col *gkvlite.Collection) error {
		j, err := json.Marshal(job)
		if err != nil {
			return &StoreError{Code: ErrFailedWrite, Err: err}
		}
		err = col.Set([]byte(job.Id), j)
		if err != nil {
			return &StoreError{Code: ErrFailedWrite, Err: err}
		}
		return bs.store.Flush()
	})
}
