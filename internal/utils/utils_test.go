package utils

import (
	"fmt"
	"testing"
)

func TestBytesToNumber(t *testing.T) {
	src := make([]byte, 2)
	src[0] = 0x0F
	src[1] = 0x01

	var num uint16
	if err := BytesToNumber(src, &num); err == nil {
		if num != 271 {
			t.Fatal(fmt.Sprintf("%d != %d", num, 271))
		}
	} else {
		t.Fatal(err)
	}
}

func TestNumberToBytes(t *testing.T) {
	var num uint16 = 271
	if src, err := NumberToBytes(num); err == nil {
		if src[0] != 0x0F && src[1] != 0x01 {
			t.Fatal(fmt.Sprintf("%v is not 271", src))
		}
	} else {
		t.Fatal(err)
	}
}
