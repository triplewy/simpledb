package gossip

import "crypto/sha1"

// HashKey hashes a string to its appropriate size.
func HashKey(key string) []byte {
	h := sha1.New()
	h.Write([]byte(key))
	v := h.Sum(nil)
	return v[:KeyLength/8]
}
