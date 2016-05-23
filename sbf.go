package sbf

// Scale Bloom Filte in Redis
// date format
// frame end padding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/appwilldev/redis_sbf/internal/murmur"
	. "github.com/appwilldev/redis_sbf/internal/utils"
	"github.com/garyburd/redigo/redis"
	"math"
	"math/rand"
	"time"
)

var (
	SBF_NAME    = "SBF"   // 3 bytes
	SBF_VERSION = "1.0.0" // 5 bytes
)

const (
	// header
	SBF_HEADER_SIZE = 18 // 18 bytes

	// frame
	SBF_FRAME_HEADER_SIZE = 6    // 6 bytes, empty now, reserve for future use
	SBF_FRAME_COUNT_LIMIT = 1024 // the frame count of the sbf
	SBF_FRAME_PADDING     = 1    // 1 byte, reserve for protect the data at the end of previus frame
	// slice
	SBF_DEFAULT_S_COUNT             = 10    // slice count
	SBF_DEFAULT_S_SIZE              = 65536 // slice size
	SBF_DEFAULT_S_ERROR_RATIO       = 0.5   // the percentage of a slice used
	SBF_DEFAULT_S_MIN_CAPACITY_SIZE = 2     // the min growth of the slice size
	SBF_DEFAULT_S_MAX_CAPACITY_SIZE = 4     // the max growth of the slice size
)

func init() {
	if len(SBF_NAME) != 3 || len(SBF_VERSION) != 5 {
		panic(errors.New("invalid sbf name or sbf version"))
	}
	rand.Seed(time.Now().UnixNano())
}

// header of SBF
type SBFHeader struct {
	Name       [3]byte // SBF                           0 -  3
	Version    [5]byte // 1.0.0                         3 -  8
	Count      uint16  // frame count                   8 - 10
	FullRate   uint16  // error ratio  =0.1 * 10000    10 - 12
	SliceCount uint16  // hash functions count         12 - 14
	SliceSize  uint32  // slice size                   14 - 18
	Refer      string
}

// sliceSize must be the multiple of 8
func NewHeader(conn redis.Conn, sliceRatio float32, sliceCount uint16, sliceSize uint32, refer string) (*SBFHeader, error) {
	header := new(SBFHeader)
	copy(header.Name[:], SBF_NAME)
	copy(header.Version[:], SBF_VERSION)
	header.Count = 1
	header.FullRate = uint16(sliceRatio * 10000)
	header.SliceCount = sliceCount
	header.SliceSize = sliceSize
	header.Refer = refer

	if header.SliceSize%8 != 0 {
		return nil, errors.New(fmt.Sprintf("%d NOT multiple of 8", sliceSize))
	}
	// save
	err := header.updateHeader(conn)
	return header, err
}

// load sbf header from redis
func LoadHeader(conn redis.Conn, refer string) (*SBFHeader, error) {
	if ret, err := redis.Bytes(conn.Do("GETRANGE", refer, 0, SBF_HEADER_SIZE-1)); err == nil {
		if len(ret) > 0 {
			header := new(SBFHeader)
			copy(header.Name[:], ret[0:3])
			copy(header.Version[:], ret[3:8])
			// from bytes to number
			if err := BytesToNumber(ret[8:10], &header.Count); err != nil {
				return nil, err
			}
			if err := BytesToNumber(ret[10:12], &header.FullRate); err != nil {
				return nil, err
			}
			if err := BytesToNumber(ret[12:14], &header.SliceCount); err != nil {
				return nil, err
			}
			if err := BytesToNumber(ret[14:18], &header.SliceSize); err != nil {
				return nil, err
			}
			header.Refer = refer
			return header, nil
		} else {
			return nil, errors.New(fmt.Sprintf("SBF %s NOT FOUND.", refer))
		}
	} else {
		return nil, err
	}
}

func (s *SBFHeader) checkHeader() error {
	if bytes.Equal(s.Name[:], []byte(SBF_NAME)) {
		return errors.New("INVALID SBF header.")
	}
	if bytes.Compare(s.Version[:], []byte(SBF_VERSION)) > 0 {
		return errors.New("NOT supported version.")
	}
	return nil
}

func (s *SBFHeader) updateHeader(conn redis.Conn) error {
	// name
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, s.Name)
	binary.Write(buf, binary.LittleEndian, s.Version)
	binary.Write(buf, binary.LittleEndian, s.Count)
	binary.Write(buf, binary.LittleEndian, s.FullRate)
	binary.Write(buf, binary.LittleEndian, s.SliceCount)
	binary.Write(buf, binary.LittleEndian, s.SliceSize)
	// write to redis
	_, err := conn.Do("SETRANGE", s.Refer, 0, buf.Bytes())
	return err
}

// update header info
// with big lock
func (s *SBFHeader) incrCount(conn redis.Conn) error {
	lockKey := fmt.Sprintf("lock:%s:count:%s", SBF_NAME, s.Refer)
	for i := 0; i < 10; i++ {
		if val, err := redis.Int(conn.Do("GET", lockKey)); err == nil && val > 0 {
			time.Sleep(time.Millisecond * 500)
		} else {
			if _, err := conn.Do("EXPIRE", lockKey, 5); err != nil {
				return err
			}
			break
		}
	}
	var count uint16
	if ret, err := redis.Bytes(conn.Do("GETRANGE", s.Refer, 8, 9)); err == nil {
		if err := BytesToNumber(ret, &count); err == nil {
			if count == s.Count {
				s.Count += 1
				if val, err := NumberToBytes(s.Count); err == nil {
					_, err := conn.Do("SETRANGE", s.Refer, 8, val)
					return err
				} else {
					return err
				}
			} else if count > s.Count {
				s.Count = count
				return nil
			} else {
				return nil
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

// SBFFrame
type SBFFrame struct {
	SliceCount uint16  // frame hash functions count
	FullRate   float32 // frame error ratio
	SliceSize  uint32  // frame capacity
	StartIndex uint32  // frame start index (bit)
	EndIndex   uint32  // frame end index (bit)
	Count      uint32  // elements inserted
	Key        string  // key: sbf:refer:frameID
	Refer      string  // refer
}

func NewFrame(conn redis.Conn, header *SBFHeader, id uint16) (*SBFFrame, error) {
	key := fmt.Sprintf("%s:count:%s:%d", SBF_NAME, header.Refer, id)
	frame := new(SBFFrame)
	frame.Key = key
	frame.Refer = header.Refer
	frame.frameDataRange(header, id)
	conn.Send("MULTI")
	conn.Send("SETBIT", frame.Refer, frame.EndIndex, 0)
	conn.Send("SET", frame.Key, 0)
	_, err := conn.Do("EXEC")
	if err == nil {
		return frame, nil
	} else {
		return nil, err
	}
}

func LoadFrame(conn redis.Conn, header *SBFHeader, id uint16) (*SBFFrame, error) {
	// key := SBF_NAME + ":count:" + header.Refer + ":1"
	key := fmt.Sprintf("%s:count:%s:%d", SBF_NAME, header.Refer, id)
	frame := new(SBFFrame)
	frame.Key = key
	frame.Refer = header.Refer
	frame.Count = 0
	frame.frameDataRange(header, id)
	if count, err := redis.Uint64(conn.Do("GET", key)); err == nil {
		frame.Count = uint32(count)
	} else if err != redis.ErrNil {
		return nil, err
	}
	return frame, nil
}

// fullfill frame fields
// according to errorRate, capacity, can get the size one bloom filter.
func (s *SBFFrame) frameDataRange(header *SBFHeader, id uint16) {
	s.FullRate = float32(header.FullRate) / 10000
	for i := 1; i <= int(id); i++ {
		s.SliceCount = uint16(math.Ceil(float64(header.SliceCount) + float64(i-1)*math.Log2(1.0/float64(SBF_DEFAULT_S_ERROR_RATIO))))
		s.SliceSize = (uint32(float64(header.SliceSize)*math.Pow(SBF_DEFAULT_S_MIN_CAPACITY_SIZE, float64(i-1))) >> 3) << 3

		s.EndIndex += (s.SliceSize*uint32(s.SliceCount) + (SBF_FRAME_HEADER_SIZE+SBF_FRAME_PADDING)<<3)
	}
	s.EndIndex += SBF_HEADER_SIZE << 3
	s.StartIndex = s.EndIndex - uint32(s.SliceCount)*s.SliceSize - (SBF_FRAME_HEADER_SIZE+SBF_FRAME_PADDING)<<3
}

func (s *SBFFrame) IsFrameFull() bool {
	return float64(s.Count) >= float64(s.FullRate)*float64(s.SliceSize)
}

func (s *SBFFrame) Add(conn redis.Conn, element []byte) bool {
	hashes := murmur.Hashes(element, s.SliceCount, s.SliceSize)
	// update bit val
	conn.Send("MULTI")
	for index, h := range hashes {
		pos := uint32(index)*s.SliceSize + s.StartIndex + h + SBF_FRAME_HEADER_SIZE<<3
		conn.Send("SETBIT", s.Refer, pos, 1)
	}
	conn.Send("INCR", s.Key)
	_, err := conn.Do("EXEC")
	if err == nil {
		s.Count += 1
		return true
	} else {
		return false
	}
}

func (s *SBFFrame) Check(conn redis.Conn, element []byte) bool {
	var flag int = 1
	hashes := murmur.Hashes(element, s.SliceCount, s.SliceSize)
	// check bit val
	conn.Send("MULTI")
	for index, h := range hashes {
		pos := uint32(index)*s.SliceSize + s.StartIndex + h + SBF_FRAME_HEADER_SIZE<<3
		conn.Send("GETBIT", s.Refer, pos)
	}
	if data, err := redis.Ints(conn.Do("EXEC")); err == nil {
		for _, f := range data {
			flag = flag & f
			if flag != 1 {
				return false
			}
		}
		return (flag == 1)
	} else {
		return false
	}
}

// SBF struct
type SBF struct {
	Header *SBFHeader
	Conn   redis.Conn
}

// new SBF
func NewSBF(conn redis.Conn, sliceRatio float32, sliceCount uint16, sliceSize uint32, refer string) (*SBF, error) {
	// check if exist
	if flag, err := redis.Bool(conn.Do("EXISTS", refer)); err == nil && !flag {
		if header, err := NewHeader(conn, sliceRatio, sliceCount, sliceSize, refer); err == nil {
			if _, err = NewFrame(conn, header, 1); err == nil {
				sbf := new(SBF)
				sbf.Conn = conn
				sbf.Header = header
				return sbf, nil
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, errors.New(fmt.Sprintf("sbf with key %s have exists.", refer))
	}
}

func LoadSBF(conn redis.Conn, refer string) (*SBF, error) {
	var err error
	sbf := new(SBF)
	if sbf.Header, err = LoadHeader(conn, refer); err != nil {
		// close
		// conn.Close()
		return nil, err
	}
	sbf.Conn = conn

	return sbf, nil
}

func TruncateSBF(conn redis.Conn, refer string) error {
	if sbf, err := LoadSBF(conn, refer); err == nil {
		for i := 0; i < int(sbf.Header.Count); i++ {
			key := fmt.Sprintf("%s:count:%s:%d", SBF_NAME, refer, i)
			// ignore errors
			conn.Do("DEL", key)
		}
		if _, err := conn.Do("DEL", refer); err == nil {
			sbf.Header.Count = 1
			sbf.Header.updateHeader(conn)
			_, err = NewFrame(conn, sbf.Header, 1)
			return err
		} else {
			return err
		}
	} else {
		return err
	}
}

// Add element to sbf
// Steps:
//  * check if sbf exists.
//  * load or create sbf.
//  * load last frame.
//  * check if last frame is fullfilled.
//  * if frame is fullfilled, create a new frame.
//  * add to this frame.
func (s *SBF) Add(element []byte) bool {
	// if !s.Check(element) {
	if frame, err := LoadFrame(s.Conn, s.Header, s.Header.Count); err == nil {
		// check if frame is full
		if frame.IsFrameFull() {
			if s.Header.Count < SBF_FRAME_COUNT_LIMIT {
				// update header
				if err := s.Header.incrCount(s.Conn); err == nil {
					if frame, err = NewFrame(s.Conn, s.Header, s.Header.Count); err != nil {
						return false
					}
				} else {
					return false
				}
			} else {
				// frames have reached the limication, use old frames.
				// this may increase the error rate.
				id := uint16(rand.Uint32() % uint32(s.Header.Count))
				frame, err = LoadFrame(s.Conn, s.Header, id)
				if err != nil {
					return false
				}
			}
		}
		return frame.Add(s.Conn, element)
	} else {
		fmt.Println(err)
		return false
	}
	//}
	// return true
}

// Check if an element belongs to this sbf
// Steps:
//  * check if sbf exists.
//  * if not, return false
//  * if yes, check the first frame.
//  * if element in this frame, return true;
//  * else load next frame and check if element in this frame in loop, until find one in or not find in all frames.
func (s *SBF) Check(element []byte) bool {
	for i := 1; i <= int(s.Header.Count); i += 1 {
		if frame, err := LoadFrame(s.Conn, s.Header, uint16(i)); err == nil {
			if frame.Check(s.Conn, element) {
				return true
			}
		}
	}
	return false
}
