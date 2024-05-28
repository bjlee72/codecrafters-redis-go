package main

import (
	"bufio"
	"fmt"
	"net"
)

// Connection represents a Redis connection between client and server.
type Connection struct {
	conn   net.Conn
	reader *bufio.Reader
	token  string
}

// NewConnection returns a new RequestLoop instance.
func NewConnection(c net.Conn) *Connection {
	return &Connection{
		conn:   c,
		reader: bufio.NewReader(c),
	}
}

// Close closes the connection
func (conn *Connection) Close() {
	conn.conn.Close()
}

// GetToken returns just one token from the given connection.
func (conn *Connection) GetToken() (string, error) {
	for {
		bytes, isPrefix, err := conn.reader.ReadLine()
		if err != nil {
			return "", fmt.Errorf("reader.Readline: %v", err)
		}

		conn.token += string(bytes)
		if !isPrefix {
			ret := conn.token
			conn.token = ""
			return ret, nil
		}
	}
}

// Write is a low-level write operation on the connection.
func (conn *Connection) Write(bytes []byte) (int, error) {
	return conn.conn.Write(bytes)
}
