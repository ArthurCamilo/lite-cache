package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	C_STRING   = '+'
	C_ERROR    = '-'
	C_INTEGER  = ':'
	C_BULK     = '$'
	C_ARRAY    = '*'
	C_CARRIAGE = '\r'
	C_NEW_LINE = '\n'
)

type RespValueType string

const (
	VT_ARRAY  RespValueType = "array"
	VT_BULK   RespValueType = "bulk"
	VT_STRING RespValueType = "string"
	VT_NULL   RespValueType = "null"
	VT_ERROR  RespValueType = "error"
)

type RespValue struct {
	typ   RespValueType
	str   string
	bulk  string
	array []RespValue
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == C_CARRIAGE {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) readArray() (RespValue, error) {
	v := RespValue{}
	v.typ = VT_ARRAY

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.array = make([]RespValue, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}

	return v, nil
}

func (r *Resp) readBulk() (RespValue, error) {
	v := RespValue{}

	v.typ = VT_BULK

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.bulk = string(bulk)

	r.readLine()

	return v, nil
}

func (r *Resp) Read() (RespValue, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return RespValue{}, err
	}

	switch _type {
	case C_ARRAY:
		return r.readArray()
	case C_BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return RespValue{}, nil
	}
}
