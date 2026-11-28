//go:build !linux

package main

import "time"

func batchEvidenceClock() (time.Time, bool) { return time.Now().UTC(), false }
