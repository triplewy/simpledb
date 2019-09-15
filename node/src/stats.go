package simpledb

import (
	"runtime"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

// Stats is a struct that contains all host stats
type Stats struct {
	totalMemory string
	usedMemory  string

	totalSpace string
	usedSpace  string

	numCores   string
	cpuPercent string

	os       string
	hostname string
	uptime   string

	lastUpdated time.Time
	err         error
}

func (node *Node) getStats() {
	stats := node.stats

	stats.os = runtime.GOOS

	// memory
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		Error.Println("Could not get machine memory stats")
		stats.err = err
		return
	}

	// disk
	diskStat, err := disk.Usage("/")
	if err != nil {
		Error.Println("Could not get machine disk stats")
		stats.err = err
		return
	}

	// cpu - get CPU number of cores and speed
	cpuStat, err := cpu.Info()
	if err != nil {
		Error.Println("Could not get machine cpu stats")
		stats.err = err
		return
	}

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		Error.Println("Could not get machine cpu percentage stats")
		stats.err = err
		return
	}

	// host or machine kernel, uptime, platform Info
	hostStat, err := host.Info()
	if err != nil {
		Error.Println("Could not get machine info stats")
		stats.err = err
		return
	}

	stats.totalMemory = strconv.FormatUint(vmStat.Total, 10)
	stats.usedMemory = strconv.FormatFloat(vmStat.UsedPercent, 'f', 2, 64)

	stats.totalSpace = strconv.FormatUint(diskStat.Total, 10)
	stats.usedSpace = strconv.FormatFloat(diskStat.UsedPercent, 'f', 2, 64)

	stats.numCores = strconv.FormatInt(int64(cpuStat[0].Cores), 10)
	stats.cpuPercent = strconv.FormatFloat(cpuPercent[0], 'f', 2, 64)

	stats.hostname = hostStat.Hostname
	stats.uptime = strconv.FormatUint(hostStat.Uptime, 10)

	stats.lastUpdated = time.Now()
	stats.err = nil

	node.stats = stats
}
