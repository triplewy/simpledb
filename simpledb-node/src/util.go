package simpledb

import (
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
