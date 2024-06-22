package protocol

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
func (c *Connection) Close() {
	c.conn.Close()
}

// Read returns just one token from the given connection.
func (c *Connection) Read() (string, error) {
	for {
		bytes, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			return "", fmt.Errorf("reader.Readline: %v", err)
		}

		c.token += string(bytes)
		if !isPrefix {
			ret := c.token
			c.token = ""
			return ret, nil
		}
	}
}

// WriteBytes is a low-level write operation on the connection.
func (c *Connection) WriteBytes(bytes []byte) (int, error) {
	return c.conn.Write(bytes)
}

// Write is a low-level write operation on the connection
func (c *Connection) Write(str string) (int, error) {
	return c.WriteBytes([]byte(str))
}
