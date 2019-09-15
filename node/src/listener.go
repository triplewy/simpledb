package simpledb

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"syscall"
	"time"
)

// LowPort is minimum of port range on Brown machines
const LowPort int = 32768

// HighPort is maximum of port range on Brown machines
const HighPort int = 61000

// WinEADDRINUSE is Errno to support windows machines
const WinEADDRINUSE = syscall.Errno(10048)

// OpenListener listens on a random port in the defined ephemeral range, retries if port is already in use
func OpenListener() (net.Listener, int, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	port := rand.Intn(HighPort-LowPort) + LowPort
	hostname, err := os.Hostname()
	if err != nil {
		return nil, -1, err
	}

	addr := fmt.Sprintf("%v:%v", hostname, port)
	conn, err := net.Listen("tcp4", addr)
	if err != nil {
		if addrInUse(err) {
			time.Sleep(100 * time.Millisecond)
			return OpenListener()
		}
		return nil, -1, err
	}
	return conn, port, err
}

// OpenTCPListener listens on a random port in the defined ephemeral range, retries if port is already in use.
// Returns a TCPListener type (instead of a generic net.Listener).
func OpenTCPListener() (*net.TCPListener, int, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	port := rand.Intn(HighPort-LowPort) + LowPort
	hostname, err := os.Hostname()
	if err != nil {
		return nil, -1, err
	}

	addr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%v:%v", hostname, port))
	if err != nil {
		return nil, -1, err
	}

	conn, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		if addrInUse(err) {
			time.Sleep(100 * time.Millisecond)
			return OpenTCPListener()
		}
		return nil, -1, err
	}
	return conn, port, err
}

func addrInUse(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if osErr, ok := opErr.Err.(*os.SyscallError); ok {
			return osErr.Err == syscall.EADDRINUSE || osErr.Err == WinEADDRINUSE
		}
	}
	return false
}
