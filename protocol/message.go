package protocol

import (
	"fmt"
	"strings"
)

var (
	/*
	 * 	Pre-defined messages.
	 */
	OK   = NewSimple("OK")
	PING = NewArray([]string{"PING"})
	PONG = NewSimple("PONG")
	NULL = NewNull()
)

type Message interface {
	// Redis returns REDIS string representation of the message.
	Redis() string

	// Propagatible returns a boolean value saying whether the message can be propagated to message.
	Propagatible() bool
}

// Propagatible commands (or requests)
var propagatible = map[string]bool{
	"SET": true,
}

type ArrayMessage struct {
	msg          string
	raw          []string
	propagatible bool
}

// NewArray returns an message, which is an array of bulk strings.
func NewArray(tokens []string) *ArrayMessage {
	if len(tokens) == 0 {
		return &ArrayMessage{
			msg:          "*0\r\n",
			raw:          []string{},
			propagatible: false,
		}
	}

	bulks := make([]string, 0, len(tokens))
	for _, blk := range tokens {
		bulks = append(bulks, fmt.Sprintf("$%d\r\n%s", len(blk), blk))
	}
	result := fmt.Sprintf("*%d\r\n%s\r\n", len(tokens), strings.Join(bulks, "\r\n"))

	return &ArrayMessage{
		msg:          result,
		raw:          tokens,
		propagatible: propagatible[strings.ToUpper(tokens[0])],
	}
}

func (sm *ArrayMessage) Raw() []string {
	return sm.raw
}

// At finds an string in raw array by idx
func (am *ArrayMessage) Token(idx int) string {
	return am.raw[idx]
}

func (am *ArrayMessage) SliceFrom(idx int) []string {
	return am.raw[idx:]
}

func (am *ArrayMessage) Len() int {
	return len(am.raw)
}

func (am *ArrayMessage) Redis() string {
	return am.msg
}

func (am *ArrayMessage) Propagatible() bool {
	return am.propagatible
}

type IntMessage struct {
	msg string
	raw int
}

func NewInt(val int) *IntMessage {
	return &IntMessage{
		msg: fmt.Sprintf(":%d\r\n", val),
		raw: val,
	}
}

func (im *IntMessage) Raw() int {
	return im.raw
}

func (im *IntMessage) Redis() string {
	return im.msg
}

func (im *IntMessage) Propagatible() bool {
	return false
}

type SimpleMessage struct {
	msg string
	raw string
}

func NewSimple(str string) *SimpleMessage {
	return &SimpleMessage{
		msg: fmt.Sprintf("+%s\r\n", str),
		raw: str,
	}
}

func (sm *SimpleMessage) Raw() string {
	return sm.raw
}

func (sm *SimpleMessage) Redis() string {
	return sm.msg
}

func (sm *SimpleMessage) Propagatible() bool {
	return false
}

type BulkMessage struct {
	msg string
	raw string
}

func NewBulk(str string) *BulkMessage {
	return &BulkMessage{
		msg: fmt.Sprintf("$%d\r\n%s\r\n", len(str), str),
		raw: str,
	}
}

func (bm *BulkMessage) Raw() string {
	return bm.raw
}

func (bm *BulkMessage) Redis() string {
	return bm.msg
}

func (bm *BulkMessage) Propagatible() bool {
	return false
}

type NullMessage struct{}

func NewNull() *NullMessage {
	return &NullMessage{}
}

func (nm *NullMessage) Redis() string {
	return "$-1\r\n"
}

func (nm *NullMessage) Propagatible() bool {
	return false
}
