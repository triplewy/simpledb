package main

import "time"

const dirPerm = 0700
const filePerm = 0600
const applyTimeout = 10 * time.Second

// Config is configuration for db
type Config struct {
	dataDir  string
	sslDir   string
	rpcPort  int
	raftPort int
}
