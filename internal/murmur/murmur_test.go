package murmur

import (
	"testing"
)

func HashCount_test(t *testing.T) {
}

func BenchmarkHashes(b *testing.B) {
	var key []byte = []byte("abcdefghijklmnopqrstuvwxyz1234567890")
	for i := 0; i < b.N; i++ {
		Hashes(key, 10, 65536)
	}
}
