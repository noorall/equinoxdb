package common

import (
	"gorm.io/gorm"
	"time"
)

type Task struct {
	gorm.Model
	FinishedAt time.Time
	Finished   bool
	Stopped    bool
	Thread     int
	Progress   int
	Type       int

	DataPath string
	DataSize int64

	WrittenSize   int64
	WrittenTotal  int64
	WriteDuration float64
}
