package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/milosgajdos83/taurus"
	"github.com/steveyen/gkvlite"
)

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
		if err := s.Flush(); err != nil {
			return nil, err
		}
	}

	return &BasicStore{
		store: s,
	}, nil
}

func (bs *BasicStore) withMutexContext(muType string, fn func() error) error {
	switch muType {
	case "rw":
		bs.mu.Lock()
		defer bs.mu.Unlock()
	case "r":
		bs.mu.RLock()
		defer bs.mu.RUnlock()
	}
	return fn()
}

func (bs *BasicStore) withColContext(colName string, fn func(col *gkvlite.Collection) error) error {
	c := bs.store.GetCollection(colName)
	if c == nil {
		return &taurus.StoreError{
			Code: taurus.ErrNotFound,
			Err:  fmt.Errorf("Collection %s does not exist", colName),
		}
	}
	return fn(c)
}

func (bs *BasicStore) withRecordContext(id string, col *gkvlite.Collection, fn func(record []byte) error) error {
	record, err := col.Get([]byte(id))
	if err != nil {
		return &taurus.StoreError{Code: taurus.ErrFailedRead, Err: err}
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
func (bs *BasicStore) AddJob(job *taurus.Job) error {
	return bs.withMutexContext("rw", func() error {
		return bs.withColContext("jobs", func(col *gkvlite.Collection) error {
			if colItemExists(job.Id, col) {
				return &taurus.StoreError{
					Code: taurus.ErrExists,
					Err:  fmt.Errorf("AddJob() error"),
				}
			}
			jobData, err := json.Marshal(job)
			if err != nil {
				return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
			}
			if err := col.Set([]byte(job.Id), jobData); err != nil {
				return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
			}
			return bs.store.Flush()
		})
	})
}

// RemoveJob() removes Taurus JOb from Taurus store
func (bs *BasicStore) RemoveJob(jobId string) error {
	return bs.withMutexContext("rw", func() error {
		return bs.withColContext("jobs", func(col *gkvlite.Collection) error {
			if colItemExists(jobId, col) {
				_, err := col.Delete([]byte(jobId))
				if err != nil {
					return &taurus.StoreError{Code: taurus.ErrFailedDelete, Err: err}
				}
				return bs.store.Flush()
			}
			return &taurus.StoreError{
				Code: taurus.ErrNotFound,
				Err:  fmt.Errorf("RemoveJob() error"),
			}
		})
	})
}

// UpdateJob() updates an existing Taurus Job
func (bs *BasicStore) UpdateJob(job *taurus.Job) error {
	return bs.withMutexContext("rw", func() error {
		return bs.withColContext("jobs", func(col *gkvlite.Collection) error {
			if colItemExists(job.Id, col) {
				j, err := json.Marshal(job)
				if err != nil {
					return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
				}
				err = col.Set([]byte(job.Id), j)
				if err != nil {
					return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
				}
				return bs.store.Flush()
			}
			return &taurus.StoreError{
				Code: taurus.ErrNotFound,
				Err:  fmt.Errorf("UpdateJob"),
			}
		})
	})
}

// GetJob() retrieves Taurus Job from Taurus store
func (bs *BasicStore) GetJob(jobId string) (*taurus.Job, error) {
	job := new(taurus.Job)
	err := bs.withMutexContext("r", func() error {
		return bs.withColContext("jobs", func(col *gkvlite.Collection) error {
			if colItemExists(jobId, col) {
				return bs.withRecordContext(jobId, col, func(record []byte) error {
					if err := json.Unmarshal(record, job); err != nil {
						return &taurus.StoreError{
							Code: taurus.ErrFailedRead,
							Err:  err,
						}
					}
					return nil
				})
			}
			return &taurus.StoreError{
				Code: taurus.ErrNotFound,
				Err:  fmt.Errorf("GetJob() error"),
			}
		})
	})
	return job, err
}

// Jobs() returns a slice of all Taurus Jobs
func (bs *BasicStore) GetAllJobs() ([]*taurus.Job, error) {
	jobs := make([]*taurus.Job, 0, 0)
	err := bs.withMutexContext("r", func() error {
		return bs.withColContext("jobs", func(col *gkvlite.Collection) error {
			var err error
			col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
				err = bs.withRecordContext(string(i.Key), col, func(record []byte) error {
					job := new(taurus.Job)
					if err := json.Unmarshal(record, job); err != nil {
						return &taurus.StoreError{
							Code: taurus.ErrFailedRead,
							Err:  err,
						}
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
	})
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// Jobs() retrieves NEW Taurus Job from Taurus store
func (bs *BasicStore) GetJobs(state taurus.State) ([]*taurus.Job, error) {
	jobs := make([]*taurus.Job, 0, 0)
	err := bs.withMutexContext("r", func() error {
		return bs.withColContext("jobs", func(col *gkvlite.Collection) error {
			var err error
			col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
				err = bs.withRecordContext(string(i.Key), col, func(record []byte) error {
					job := new(taurus.Job)
					if err := json.Unmarshal(record, job); err != nil {
						return &taurus.StoreError{
							Code: taurus.ErrFailedRead,
							Err:  err,
						}
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
	})
	if err != nil {
		return nil, err
	}

	return jobs, nil
}
