package simpledb

import (
	"os"
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
}

func NewConfig() *Config {
	addrString := os.Getenv("DISCOVERY_ADDRESSES")
	addrArr := strings.Split(addrString, ",")

	return &Config{
		RpcPort:          "7000",
		HttpPort:         "8000",
		RaftPort:         "9000",
		RaftDir:          "./data/raft",
		DbDir:            "./data/simpledb",
		DiscoveryTimeout: 20,
		DiscoveryAddrs:   addrArr,
	}
}
