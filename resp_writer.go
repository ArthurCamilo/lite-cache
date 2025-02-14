package main

import (
	"io"
	"strconv"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v RespValue) error {
	var bytes = v.WriteBytes()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (v RespValue) WriteBytes() []byte {
	switch v.typ {
	case VT_ARRAY:
		return v.fromArray()
	case VT_BULK:
		return v.fromBulk()
	case VT_STRING:
		return v.fromString()
	case VT_NULL:
		return v.fromNull()
	case VT_ERROR:
		return v.fromError()
	default:
		return []byte{}
	}
}

func (v RespValue) fromString() []byte {
	var bytes []byte
	bytes = append(bytes, C_STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, C_CARRIAGE, C_NEW_LINE)

	return bytes
}

func (v RespValue) fromBulk() []byte {
	var bytes []byte
	bytes = append(bytes, C_BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, C_CARRIAGE, C_NEW_LINE)
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, C_CARRIAGE, C_NEW_LINE)

	return bytes
}

func (v RespValue) fromArray() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, C_ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, C_CARRIAGE, C_NEW_LINE)

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].WriteBytes()...)
	}

	return bytes
}

func (v RespValue) fromError() []byte {
	var bytes []byte
	bytes = append(bytes, C_ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, C_CARRIAGE, C_NEW_LINE)

	return bytes
}

func (v RespValue) fromNull() []byte {
	return []byte("$-1\r\n")
}
