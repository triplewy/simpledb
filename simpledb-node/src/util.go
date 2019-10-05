package simpledb

import (
	"bytes"
	"crypto/sha1"
	"log"
	"net"
)

// HashKey hashes a string to its appropriate size.
func HashKey(key string) []byte {
	h := sha1.New()
	h.Write([]byte(key))
	v := h.Sum(nil)
	return v[:KeyLength/8]
}

// GetOutboundIP gets preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// Max returns the larger of x or y.
func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func bytesCompareLess(a, b []byte) bool {
	return bytes.Compare(a, b) == -1
}

func bytesCompareGreater(a, b []byte) bool {
	return bytes.Compare(a, b) == 1
}

func bytesCompareEqual(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}
