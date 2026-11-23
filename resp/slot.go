package resp

import "strings"

// SlotCount is the number of hash slots in a Redis Cluster.
const SlotCount = 16384

// HashTag returns the part of key that Redis Cluster hashes: the substring
// between the first '{' and the next '}', if that substring is not empty,
// and the whole key otherwise.
func HashTag(key string) string {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j > 0 {
			return key[i+1 : i+1+j]
		}
	}
	return key
}

// Slot returns the Redis Cluster hash slot of key: CRC16-XMODEM of its
// hash tag, modulo 16384.
func Slot(key string) int {
	return int(crc16(HashTag(key)) % SlotCount)
}

func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
