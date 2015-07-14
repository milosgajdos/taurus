package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/milosgajdos83/taurus/taurus"
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
	}

	if s.GetCollection("tasks") == nil {
		s.SetCollection("tasks", nil)
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
	return bs.withColContext("rw", "jobs", func(col *gkvlite.Collection) error {
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
}

// RemoveJob() removes Taurus JOb from Taurus store
func (bs *BasicStore) RemoveJob(jobId string) error {
	return bs.withColContext("rw", "jobs", func(col *gkvlite.Collection) error {
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
}

// GetJob() retrieves Taurus Job from Taurus store
func (bs *BasicStore) GetJob(jobId string) (*taurus.Job, error) {
	job := new(taurus.Job)
	err := bs.withColContext("r", "jobs", func(col *gkvlite.Collection) error {
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
	return job, err
}

// Jobs() returns a slice of all Taurus Jobs
func (bs *BasicStore) GetAllJobs() ([]*taurus.Job, error) {
	jobs := make([]*taurus.Job, 0, 0)
	err := bs.withColContext("r", "jobs", func(col *gkvlite.Collection) error {
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
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// Jobs() retrieves NEW Taurus Job from Taurus store
func (bs *BasicStore) GetJobs(state taurus.State) ([]*taurus.Job, error) {
	jobs := make([]*taurus.Job, 0, 0)
	err := bs.withColContext("r", "jobs", func(col *gkvlite.Collection) error {
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
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// UpdateJob() updates an existing Taurus Job
func (bs *BasicStore) UpdateJob(job *taurus.Job) error {
	return bs.withColContext("rw", "jobs", func(col *gkvlite.Collection) error {
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
}

func (bs *BasicStore) AddTask(task *taurus.Task) error {
	return bs.withColContext("rw", "tasks", func(col *gkvlite.Collection) error {
		taskId := task.Info.TaskId.GetValue()
		if colItemExists(taskId, col) {
			return &taurus.StoreError{
				Code: taurus.ErrExists,
				Err:  fmt.Errorf("AddTask() eror"),
			}
		}
		taskData, err := json.Marshal(task)
		if err != nil {
			return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
		}
		if err := col.Set([]byte(taskId), taskData); err != nil {
			return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
		}
		return bs.store.Flush()
	})
}

func (bs *BasicStore) RemoveTask(taskId string) error {
	return bs.withColContext("rw", "tasks", func(col *gkvlite.Collection) error {
		if colItemExists(taskId, col) {
			_, err := col.Delete([]byte(taskId))
			if err != nil {
				return &taurus.StoreError{Code: taurus.ErrFailedDelete, Err: err}
			}
			return bs.store.Flush()
		}
		return &taurus.StoreError{
			Code: taurus.ErrNotFound,
			Err:  fmt.Errorf("RemoveTask() error"),
		}
	})
}

func (bs *BasicStore) GetTask(taskId string) (*taurus.Task, error) {
	task := new(taurus.Task)
	err := bs.withColContext("r", "tasks", func(col *gkvlite.Collection) error {
		if colItemExists(taskId, col) {
			return bs.withRecordContext(taskId, col, func(record []byte) error {
				if err := json.Unmarshal(record, task); err != nil {
					return &taurus.StoreError{Code: taurus.ErrFailedRead, Err: err}
				}
				return nil
			})
		}
		return &taurus.StoreError{
			Code: taurus.ErrNotFound,
			Err:  fmt.Errorf("GetTask() error"),
		}
	})
	return task, err
}

func (bs *BasicStore) UpdateTask(task *taurus.Task) error {
	return bs.withColContext("rw", "tasks", func(col *gkvlite.Collection) error {
		taskId := task.Info.TaskId.GetValue()
		if colItemExists(taskId, col) {
			t, err := json.Marshal(task)
			if err != nil {
				return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
			}
			err = col.Set([]byte(taskId), t)
			if err != nil {
				return &taurus.StoreError{Code: taurus.ErrFailedWrite, Err: err}
			}
			return bs.store.Flush()
		}
		return &taurus.StoreError{
			Code: taurus.ErrNotFound,
			Err:  fmt.Errorf("UpdateTask() error"),
		}
	})
}

func (bs *BasicStore) GetJobTasks(jobId string) ([]*taurus.Task, error) {
	tasks := make([]*taurus.Task, 0, 0)
	err := bs.withColContext("r", "tasks", func(col *gkvlite.Collection) error {
		var err error
		col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
			err = bs.withRecordContext(string(i.Key), col, func(record []byte) error {
				task := new(taurus.Task)
				if err := json.Unmarshal(record, task); err != nil {
					return &taurus.StoreError{Code: taurus.ErrFailedRead, Err: err}
				}
				if task.JobId == jobId {
					tasks = append(tasks, task)
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

	return tasks, nil
}

func (bs *BasicStore) GetTasks(state taurus.State) ([]*taurus.Task, error) {
	tasks := make([]*taurus.Task, 0, 0)
	err := bs.withColContext("r", "tasks", func(col *gkvlite.Collection) error {
		var err error
		col.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
			err = bs.withRecordContext(string(i.Key), col, func(record []byte) error {
				task := new(taurus.Task)
				if err := json.Unmarshal(record, task); err != nil {
					return &taurus.StoreError{
						Code: taurus.ErrFailedRead,
						Err:  err,
					}
				}
				if task.State == state {
					tasks = append(tasks, task)
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

	return tasks, nil
}
