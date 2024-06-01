package main

var cache map[string]entry

type entry struct {
	value    string
	expireAt int64
}

func init() {
	cache = make(map[string]entry)
}
