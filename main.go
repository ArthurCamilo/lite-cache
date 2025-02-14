package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.ConsumeFile()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()

		writer := NewWriter(conn)

		if err != nil {
			writer.Write(RespValue{typ: VT_ERROR, str: "Invalid Request - unable to parse arguments"})
			return
		}

		if value.typ != "array" {
			writer.Write(RespValue{typ: VT_ERROR, str: "Invalid Request - expected array of arguments"})
			continue
		}

		if len(value.array) == 0 {
			writer.Write(RespValue{typ: VT_ERROR, str: "Invalid Request - expected at least 1 argument"})
			continue
		}

		command := Command(strings.ToUpper(value.array[0].bulk))
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			writer.Write(RespValue{typ: VT_ERROR, str: "Invalid Command - expected one of the following: PING, SET, GET, HSET, HGET, HGETALL"})
			continue
		}

		if command == SET || command == HSET {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
