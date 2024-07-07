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
	offset uint64
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

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Offset() uint64 {
	return c.offset
}

// Read returns just one token from the given connection.
// The second return value is the total number of bytes read from the connection.
func (c *Connection) Read() (ret string, _ error) {
	defer func() {
		// c.offset is about how much we read from this connection. '2' is for \r\n.
		c.offset += uint64(len(ret) + 2)
	}()

	for {
		bytes, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			return "", fmt.Errorf("reader.Readline: %w", err)
		}

		c.token += string(bytes)
		if !isPrefix {
			ret := c.token
			c.token = ""
			return ret, nil
		}
	}
}

func (c *Connection) ReadBytes(buf []byte) (r int, _ error) {
	defer func() {
		c.offset += uint64(r)
	}()

	r, err := c.reader.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("c.reader.Read: %w", err)
	}
	return r, nil
}

func (c *Connection) WriteBytes(bytes []byte) error {
	var written int
	for written < len(bytes) {
		n, err := c.conn.Write(bytes)
		if err != nil {
			return fmt.Errorf("c.conn.Write failed: %w", err)
		}
		written += n
	}
	return nil
}

func (c *Connection) WriteString(str string) error {
	return c.WriteBytes([]byte(str))
}

func (c *Connection) Write(msg Message) error {
	return c.WriteString(msg.Redis())
}
