package utils

import (
	"bytes"
	"encoding/binary"
)

func BytesToNumber(src []byte, dest interface{}) error {
	buf := bytes.NewReader(src)
	return binary.Read(buf, binary.LittleEndian, dest)
}

func NumberToBytes(src interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := binary.Write(&buf, binary.LittleEndian, src); err == nil {
		return buf.Bytes(), nil
	} else {
		return nil, err
	}
}
