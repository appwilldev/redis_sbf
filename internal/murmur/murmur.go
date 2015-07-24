package murmur

/*
#include "murmur.h"
*/
import "C"

import (
	"unsafe"
)

const (
	MURMUR_SEED uint32 = 0x97c29b3a
)

// each call will generate 4 hash values with type uint32
func MurmurHash3_x64_128(key []byte) [4]uint32 {
	var checksum [4]uint32
	C.MurmurHash3_x64_128(unsafe.Pointer(&key[0]), C.int(len(key)), C.uint32_t(MURMUR_SEED), unsafe.Pointer(&checksum))
	return checksum
}

func Hashes(key []byte, sliceCount uint16, sliceSize uint32) []uint32 {
	ret := make([]uint32, sliceCount)

	hs := MurmurHash3_x64_128(key)
	var i uint32
	for i = 0; i < uint32(sliceCount); i += 1 {
		ret[i] = (hs[0]*i + hs[1]*(i+1) + hs[2]*(i+2) + hs[3]*(i+3)) % sliceSize
	}
	return ret
}
