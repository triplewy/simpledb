package simpledb

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	RpcPort          string
	HttpPort         string
	RaftPort         string
	RaftDir          string
	DbDir            string
	DiscoveryTimeout int
	DiscoveryAddrs   []string
	MinNumberNodes   int
}

func NewConfig() *Config {
	addrString := os.Getenv("DISCOVERY_ADDRESSES")
	addrArr := strings.Split(addrString, ",")

	minNumberNodes, err := strconv.Atoi(os.Getenv("MIN_NUMBER_NODES"))
	if err != nil {
		Error.Fatalf("Incorrect input for minimum number of nodes: %v", err)
	}

	return &Config{
		RpcPort:          "7000",
		HttpPort:         "8000",
		RaftPort:         "9000",
		RaftDir:          "./data/raft",
		DbDir:            "./data/simpledb",
		DiscoveryTimeout: 20,
		DiscoveryAddrs:   addrArr,
		MinNumberNodes:   minNumberNodes,
	}
}
