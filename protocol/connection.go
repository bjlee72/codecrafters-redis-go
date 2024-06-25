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
func (c *Connection) WriteBytes(bytes []byte) error {
	var written int
	for written < len(bytes) {
		n, err := c.conn.Write(bytes)
		if err != nil {
			return fmt.Errorf("c.conn.Write failed: %v", err)
		}
		written += n
	}
	return nil
}

// Write is a low-level write operation on the connection
func (c *Connection) Write(str string) error {
	return c.WriteBytes([]byte(str))
}
