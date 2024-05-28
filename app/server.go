package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var cache map[string]string

func init() {
	cache = make(map[string]string)
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		conn := NewConnection(c)

		// fork goroutine
		go requestHandlingLoop(conn)
	}
}

func requestHandlingLoop(conn *Connection) {
	defer conn.Close()

	for {
		token, err := conn.GetToken()
		if err != nil {
			fmt.Fprintf(os.Stderr, "conn.GetToken(): %v", err)
			return
		}

		num, err := ValidateArray(token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ValidateArray(): %v", err)
			return
		}

		requestArray := make([]string, 0, num)
		for i := 0; i < num; i++ {
			token, err := conn.GetToken()
			if err != nil {
				fmt.Fprintf(os.Stderr, "conn.GetToken(): %v", err)
				return
			}

			l, err := ValidateBulkString(token)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ValidateString(): %v", err)
				return
			}

			str, err := conn.GetToken()
			if err != nil {
				fmt.Fprintf(os.Stderr, "conn.GetToken(): %v", err)
				return
			}
			if l != len(str) {
				fmt.Fprintf(os.Stderr, "bulk string length mismatch: %v != %v", l, len(str))
				return
			}

			requestArray = append(requestArray, str)
		}

		// requestArray is a single request from a client.
		if err := handleRequest(conn, requestArray); err != nil {
			fmt.Fprintf(os.Stderr, "handleRequest failed: %v", err)
			return
		}
	}
}

func handleRequest(conn *Connection, requestArray []string) error {
	cmd := strings.ToLower(requestArray[0])

	switch cmd {

	case "ping":
		err := handlePing(conn)
		if err != nil {
			return fmt.Errorf("handlePing: %v", err)
		}

	case "echo":
		err := handleEcho(conn, requestArray[1])
		if err != nil {
			return fmt.Errorf("handleEcho: %v", err)
		}

	case "set":
		err := handleSet(conn, requestArray[1], requestArray[2])
		if err != nil {
			return fmt.Errorf("handleSet: %v", err)
		}

	case "get":
		err := handleGet(conn, requestArray[1])
		if err != nil {
			return fmt.Errorf("handleGet: %v", err)
		}
	}

	return nil
}

func handlePing(conn *Connection) error {
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func handleEcho(conn *Connection, val string) error {
	ret := fmt.Sprintf("$%v\r\n%v\r\n", len(val), val)

	_, err := conn.Write([]byte(ret))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func handleSet(conn *Connection, key string, val string) error {
	cache[key] = val

	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func handleGet(conn *Connection, key string) error {
	ret := "$-1\r\n"
	if val, ok := cache[key]; ok {
		ret = fmt.Sprintf("$%v\r\n%v\r\n", len(val), val)
	}

	_, err := conn.Write([]byte(ret))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}
