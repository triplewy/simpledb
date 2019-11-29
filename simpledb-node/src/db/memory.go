package db

import (
	"fmt"
	"runtime"
	"time"
)

type Memory struct {
	currMemory int
	maxMemory  int
}

func NewMemory() *Memory {
	m := &Memory{
		currMemory: 0,
		maxMemory:  10,
	}
	go m.run()
	return m
}

func (m *Memory) run() {
	checkMemory := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-checkMemory.C:
			PrintMemUsage()
		}
	}
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
