package queue

import (
	"os"
	"testing"
)

var bq *BasicQueue

func TestMain(m *testing.M) {
	ret := m.Run()
	os.Exit(ret)
}
