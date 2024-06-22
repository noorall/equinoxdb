package storage

import (
	"equinox/pkg/models"
	equinox "equinox/types"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
	"time"
)

func getTestDB(t *testing.T) *Engine {
	dir, err := ioutil.TempDir("/Users/noorall/GolandProjects/equinox/benchmark/write/db", "equinox-test")
	require.NoError(t, err)

	options := equinox.DefaultOptions(dir)
	options.MemTableSize = 10
	db, err := NewEngine(&options)
	require.NoError(t, err)

	return db
}

func TestWritePoints(t *testing.T) {
	e := getTestDB(t)
	fields := models.Fields{
		"field1": 1,
		"field4": 3,
	}
	points := make([]models.Point, 0)
	p, _ := models.NewPoint("hh", fields, time.Now())
	fields2 := models.Fields{
		"field2": 2,
	}
	p2, _ := models.NewPoint("hh", fields2, time.Now())
	points = append(points, p)
	points = append(points, p2)
	err := e.WritePoints(points)
	err = e.WritePoints(points)
	require.NoError(t, err)
}
