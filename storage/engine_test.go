package storage

import (
	"context"
	"equinox/pkg/models"
	"equinox/storage/config"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

func getTestDB(t *testing.T) *Engine {
	dir, err := ioutil.TempDir("/Users/noorall/GolandProjects/equinox/benchmark/write/db", "equinox-test")
	require.NoError(t, err)

	options := config.NewOption()
	options.Dir = dir
	db, err := NewEngine(options)
	require.NoError(t, err)
	return db
}

func TestWritePoints(t *testing.T) {
	e := getTestDB(t)
	_ = e.Open(context.Background())
	line := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz--\n" // 64 bytes
	var builder strings.Builder
	for i := 0; i < 64; i++ {
		builder.WriteString(line)
	}
	data := builder.String()
	fields := models.Fields{
		"field1": data,
		"field4": data,
	}
	for i := 0; i < 2621440/2; i++ {
		p, _ := models.NewPoint("hh", fields, time.Now())
		_ = e.WritePoints([]models.Point{p})
	}
	err := e.Close()
	require.NoError(t, err)
}
