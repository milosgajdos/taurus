package store_test

import (
	"os"
	"testing"

	"github.com/milosgajdos83/taurus/store"
)

var bs *store.BasicStore

func Test_AddJob(t *testing.T) {
}

func Test_RemoveJob(t *testing.T) {
}

func Test_UpdateJob(t *testing.T) {
}

func Test_GetJob(t *testing.T) {
}

func Test_GetJobs(t *testing.T) {
}

func Test_GetAllJobs(t *testing.T) {
}

func TestMain(m *testing.M) {
	var err error
	fileName := "/tmp/taurus.db"
	bs, err = store.NewBasicStore(fileName)
	if err != nil {
		panic(err)
	}
	ret := m.Run()
	os.Remove(fileName)
	os.Exit(ret)
}
