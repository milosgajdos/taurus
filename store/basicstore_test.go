package store

import (
	"os"
	"reflect"
	"testing"

	"github.com/milosgajdos/taurus"
)

var bs *BasicStore

func Test_AddJob(t *testing.T) {
	jobId := "AddJob"
	job1 := &taurus.Job{Id: jobId, State: taurus.PENDING}
	job2 := &taurus.Job{Id: jobId, State: taurus.PENDING}

	if err := bs.AddJob(job1); err != nil {
		t.Fatalf("Adding %s job failed: %v", jobId, err)
	}

	if err := bs.AddJob(job2); err != nil {
		if serr, ok := err.(*taurus.StoreError); ok {
			if serr.Code != taurus.ErrExists {
				t.Fatalf("Adding %s job failed: %v", jobId, err)
			}
		}
	}

	if err := bs.RemoveJob(jobId); err != nil {
		t.Fatalf("Removing job failed: %v", err)
	}
}

func Test_RemoveJob(t *testing.T) {
	jobId := "RemoveJob"
	job := &taurus.Job{Id: jobId, State: taurus.PENDING}

	if err := bs.AddJob(job); err != nil {
		t.Fatalf("Adding %s job failed: %v", jobId, err)
	}

	if err := bs.RemoveJob(jobId); err != nil {
		t.Fatalf("Removing job failed: %v", err)
	}

	if err := bs.RemoveJob(jobId); err != nil {
		if serr, ok := err.(*taurus.StoreError); ok {
			if serr.Code != taurus.ErrNotFound {
				t.Fatalf("Removing job failed: %v", err)
			}
		}
	}
}

func Test_UpdateJob(t *testing.T) {
	jobId := "UpdateJob"
	job1 := &taurus.Job{Id: jobId, State: taurus.PENDING}
	job2 := &taurus.Job{Id: jobId, State: taurus.RUNNING}

	if err := bs.AddJob(job1); err != nil {
		t.Fatalf("Adding %s job failed: %v", jobId, err)
	}

	if err := bs.UpdateJob(job2); err != nil {
		t.Fatalf("Updating %s job failed: %v", jobId, err)
	}

	job2.Id = "CantUpdateJob"
	if err := bs.UpdateJob(job2); err != nil {
		if serr, ok := err.(*taurus.StoreError); ok {
			if serr.Code != taurus.ErrNotFound {
				t.Fatalf("Updating %s job failed: %v", jobId, err)
			}
		}
	}

	if err := bs.RemoveJob(jobId); err != nil {
		t.Fatalf("Removing job failed: %v", err)
	}
}

func Test_GetJob(t *testing.T) {
	jobId := "GetJob"
	testJob := &taurus.Job{Id: jobId, State: taurus.PENDING}

	if err := bs.AddJob(testJob); err != nil {
		t.Fatalf("Adding %s job failed: %v", jobId, err)
	}

	gotJob, err := bs.GetJob(jobId)
	if err != nil {
		t.Fatalf("Getting %s job failed: %v", jobId, err)
	}

	eq := reflect.DeepEqual(testJob, gotJob)
	if !eq {
		t.Fatalf("GetJob test failed")
	}

	randJobId := "CantGetJob"
	if _, err := bs.GetJob(randJobId); err != nil {
		if serr, ok := err.(*taurus.StoreError); ok {
			if serr.Code != taurus.ErrNotFound {
				t.Fatalf("Getting %s job failed: %v", randJobId, err)
			}
		}
	}

	if err := bs.RemoveJob(jobId); err != nil {
		t.Fatalf("Removing job failed: %v", err)
	}
}

func Test_GetJobs(t *testing.T) {
	testJobs := []*taurus.Job{
		&taurus.Job{Id: "job1", State: taurus.PENDING},
		&taurus.Job{Id: "job2", State: taurus.PENDING},
		&taurus.Job{Id: "job3", State: taurus.RUNNING},
	}

	for _, job := range testJobs {
		if err := bs.AddJob(job); err != nil {
			t.Fatalf("Adding %s job failed: %v", job.Id, err)
		}
	}

	gotJobs, err := bs.GetJobs(taurus.PENDING)
	if err != nil {
		t.Fatalf("Getting jobs in state %s failed: %v", taurus.PENDING, err)
	}

	if len(gotJobs) != 2 {
		t.Fatalf("Getting jobs did not return all jobs in %s state", taurus.PENDING)
	}
	for _, gotJob := range gotJobs {
		if gotJob.State != taurus.PENDING {
			t.Fatalf("Job %s not in required %s state", gotJob.Id, taurus.PENDING)
		}
	}

	for _, job := range testJobs {
		if err := bs.RemoveJob(job.Id); err != nil {
			t.Fatalf("Removing job failed: %v", err)
		}
	}
}

func Test_GetAllJobs(t *testing.T) {
	testJobs := []*taurus.Job{
		&taurus.Job{Id: "job1", State: taurus.PENDING},
		&taurus.Job{Id: "job2", State: taurus.PENDING},
		&taurus.Job{Id: "job3", State: taurus.RUNNING},
	}

	for _, job := range testJobs {
		if err := bs.AddJob(job); err != nil {
			t.Fatalf("Adding %s job failed: %v", job.Id, err)
		}
	}

	gotJobs, err := bs.GetAllJobs()
	if err != nil {
		t.Fatalf("Getting all jobs failed: %v", err)
	}

	if len(gotJobs) != 3 {
		t.Fatalf("Getting jobs did not return all jobs in %s state", taurus.PENDING)
	}
	for _, gotJob := range gotJobs {
		if err := bs.RemoveJob(gotJob.Id); err != nil {
			t.Fatalf("Removing job failed: %v", err)
		}
	}
}

func TestMain(m *testing.M) {
	var err error
	fileName := "/tmp/taurus.db"
	bs, err = NewBasicStore(fileName)
	if err != nil {
		panic(err)
	}
	ret := m.Run()
	os.Remove(fileName)
	os.Exit(ret)
}
