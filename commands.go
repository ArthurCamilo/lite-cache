package main

import (
	"sync"
)

type Command string

const (
	PING    Command = "PING"
	SET     Command = "SET"
	GET     Command = "GET"
	HSET    Command = "HSET"
	HGET    Command = "HGET"
	HGETALL Command = "HGETALL"
)

var Handlers = map[Command]func([]RespValue) RespValue{
	PING:    ping,
	SET:     set,
	GET:     get,
	HSET:    hset,
	HGET:    hget,
	HGETALL: hgetall,
}

func ping(args []RespValue) RespValue {
	if len(args) == 0 {
		return RespValue{typ: VT_STRING, str: "PONG"}
	}

	return RespValue{typ: VT_STRING, str: args[0].bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []RespValue) RespValue {
	if len(args) != 2 {
		return RespValue{typ: VT_ERROR, str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return RespValue{typ: VT_STRING, str: "OK"}
}

func get(args []RespValue) RespValue {
	if len(args) != 1 {
		return RespValue{typ: VT_ERROR, str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return RespValue{typ: VT_NULL}
	}

	return RespValue{typ: VT_BULK, bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []RespValue) RespValue {
	if len(args) != 3 {
		return RespValue{typ: VT_ERROR, str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return RespValue{typ: VT_STRING, str: "OK"}
}

func hget(args []RespValue) RespValue {
	if len(args) != 2 {
		return RespValue{typ: VT_ERROR, str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return RespValue{typ: VT_NULL}
	}

	return RespValue{typ: VT_BULK, bulk: value}
}

func hgetall(args []RespValue) RespValue {
	if len(args) != 1 {
		return RespValue{typ: VT_ERROR, str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	values_map, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return RespValue{typ: VT_NULL}
	}

	values := make([]RespValue, 0, len(values_map))
	for _, v := range values_map {
		values = append(values, RespValue{typ: VT_STRING, str: v})
	}

	return RespValue{typ: VT_ARRAY, array: values}
}
