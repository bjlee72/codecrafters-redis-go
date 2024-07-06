package protocol

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type MessageType int

const (
	None MessageType = iota
	Array
	Simple
	Int
)

type Message struct {
	tokens []string
	typ    MessageType
	// for caching
	redis string
}

// NewArray returns an message, which is an array of bulk strings.
func NewArray(tokens []string) *Message {
	return &Message{
		tokens: tokens,
		typ:    Array,
	}
}

func NewInt(val int) *Message {
	return &Message{
		tokens: []string{strconv.FormatInt(int64(val), 10)},
		typ:    Int,
	}
}

var eligibleForPropagation = map[string]bool{
	"SET": true,
}

func (m *Message) EligibleForPropagation() bool {
	return propagateCommand[strings.ToUpper(m.tokens[0])]
}

func (m *Message) ToR2() string {
	switch m.typ {
	case Array:
		if len(m.redis) > 0 {
			return m.redis
		}
		bulks := make([]string, 0, len(m.tokens))
		for _, blk := range m.tokens {
			bulks = append(bulks, fmt.Sprintf("$%d\r\n%s", len(blk), blk))
		}
		m.redis = fmt.Sprintf("*%d\r\n%s\r\n", len(m.tokens), strings.Join(bulks, "\r\n"))
		return m.redis
	case Int:
		if len(m.redis) > 0 {
			return m.redis
		}
		m.redis = fmt.Sprintf(":%s\r\n", m.tokens[0])
		return m.redis
	}

	log.Fatal("not implemented")
	return ""
}
