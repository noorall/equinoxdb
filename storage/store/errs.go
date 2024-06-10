package store

import "fmt"

var (
	// ErrTSMClosed is returned when performing an operation against a closed TSM file.
	ErrTSMClosed = fmt.Errorf("tsm file closed")
)
