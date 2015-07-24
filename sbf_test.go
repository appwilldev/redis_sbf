package sbf

// SBF test
// Steps:
// * create SBF
//   ** load SBF header
//   ** the header is ok?
// * add/check element
//   ** the frame header is ok?
//   ** the element is added?
//   ** the element been added is in?
//   ** the element not been added is in?

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"testing"
)

const (
	SBF_SLICE_RATIO float32 = 0.5
	SBF_SLICE_COUNT uint16  = 10
	SBF_SLICE_SIZE  uint32  = 65536
	SBF_REFER       string  = "test"
)

func RedisConn(tb testing.TB) redis.Conn {
	if conn, err := redis.Dial("tcp", ":6379"); err != nil {
		tb.Fatal("init redis conn faield.")
		return nil
	} else {
		return conn
	}
}

func TestNewSBF(t *testing.T) {
	conn := RedisConn(t)
	defer conn.Close()

	if sbf, err := NewSBF(conn, SBF_SLICE_RATIO, SBF_SLICE_COUNT, SBF_SLICE_SIZE, SBF_REFER); err == nil {
		if sbfLoad, err := LoadSBF(conn, SBF_REFER); err == nil {
			if sbf.Header.Name != sbfLoad.Header.Name {
				t.Fatalf("name %s != %s", sbf.Header.Name, sbfLoad.Header.Name)
			}
			if bytes.Compare(sbf.Header.Version[:], sbfLoad.Header.Version[:]) != 0 {
				t.Fatalf("name %v != %v", sbf.Header.Version, sbfLoad.Header.Version)
			}
			if sbf.Header.FullRate != sbfLoad.Header.FullRate {
				t.Fatalf("fullrate %f != %f", sbf.Header.FullRate, sbfLoad.Header.FullRate)
			}
			if sbf.Header.SliceCount != sbfLoad.Header.SliceCount {
				t.Fatalf("sliceCount %d != %d", sbf.Header.SliceCount, sbfLoad.Header.SliceCount)
			}
			if sbf.Header.SliceSize != sbfLoad.Header.SliceSize {
				t.Fatalf("sliceSize %d != %d", sbf.Header.SliceSize, sbfLoad.Header.SliceSize)
			}
		} else {
			t.Fatal(err)
		}
	} else {
		t.Fatal(err)
	}
}

func TestAddElement(t *testing.T) {
	conn := RedisConn(t)
	defer conn.Close()

	if sbf, err := LoadSBF(conn, SBF_REFER); err == nil {
		var i uint64
		for i = 0; i < 32763; i++ {
			key := strconv.FormatUint(i, 10)
			if !sbf.Add([]byte(key)) {
				t.Fatal(errors.New("add failed"))
			}
		}
	} else {
		t.Fatal(err)
	}
}

func TestCheckElement(t *testing.T) {
	conn := RedisConn(t)
	defer conn.Close()

	var (
		inCount    = 0
		notInCount = 0
	)

	if sbf, err := LoadSBF(conn, SBF_REFER); err == nil {
		var i uint64
		for i = 0; i < 32768; i++ {
			key := strconv.FormatUint(i, 10)
			if sbf.Check([]byte(key)) {
				inCount += 1
			} else {
				notInCount += 1
			}
		}
		if notInCount != 0 {
			t.Log(fmt.Sprintf("%d checked in, %d checked not in.", inCount, notInCount))
		}

		inCount = 0
		notInCount = 0
		for i = 32768; i < 65536; i++ {
			key := strconv.FormatUint(i, 10)
			if sbf.Check([]byte(key)) {
				inCount += 1
			} else {
				notInCount += 1
			}
		}
		if inCount != 0 {
			t.Log(fmt.Sprintf("%d checked in, %d checked not in.", inCount, notInCount))
		}
	} else {
		t.Fatal(err)
	}
}

func BenchmarkAddlement(b *testing.B) {
	conn := RedisConn(b)
	defer conn.Close()

	if sbf, err := LoadSBF(conn, SBF_REFER); err == nil {
		var i uint64
		for i = 0; i < uint64(b.N); i++ {
			key := strconv.FormatUint(i, 10)
			sbf.Check([]byte(key))
		}
	} else {
		b.Fatal(err)
	}
}

func BenchmarkChecklement(b *testing.B) {
	conn := RedisConn(b)
	defer conn.Close()

	if sbf, err := LoadSBF(conn, SBF_REFER); err == nil {
		var i uint64
		for i = 0; i < uint64(b.N); i++ {
			key := strconv.FormatUint(i, 10)
			sbf.Check([]byte(key))
		}
	} else {
		b.Fatal(err)
	}
}
