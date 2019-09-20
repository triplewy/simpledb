package simpledb

import (
	"os"
	"strings"
)

type Config struct {
	rpcPort          int
	httpPort         int
	raftPort         int
	raftDir          string
	dbDir            string
	discoveryTimeout int
	discoveryAddrs   []string
}

func NewConfig() *Config {
	addrString := os.Getenv("DISCOVERY_ADDRESSES")
	addrArr := strings.Split(addrString, ",")

	return &Config{
		rpcPort:          50051,
		httpPort:         11000,
		raftPort:         12000,
		raftDir:          "./data/raft",
		dbDir:            "./data/simpledb",
		discoveryTimeout: 20,
		discoveryAddrs:   addrArr,
	}
}
