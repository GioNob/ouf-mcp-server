package main

import (
	"syscall"
	"time"
)

func batchEvidenceClock() (time.Time, bool) {
	var status syscall.Timex
	state, err := syscall.Adjtimex(&status)
	const timeError, statusUnsynchronized = 5, 0x40
	return time.Now().UTC(), err == nil && state != timeError && status.Status&statusUnsynchronized == 0
}
