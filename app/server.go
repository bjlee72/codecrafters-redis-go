package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

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
	cmd := strings.ToUpper(requestArray[0])

	switch cmd {

	case "PING":
		err := handlePing(conn)
		if err != nil {
			return fmt.Errorf("handlePing: %v", err)
		}

	case "ECHO":
		err := handleEcho(conn, requestArray[1])
		if err != nil {
			return fmt.Errorf("handleEcho: %v", err)
		}

	case "SET":
		options := map[string][]string{}
		var err error
		if len(requestArray) > 3 {
			options, err = buildOptions(
				requestArray[3:],
				optionConfig{"EX": 1, "PX": 1, "EXAT": 1, "PXAT": 1, "NX": 0, "XX": 0, "KEEPTTL": 0, "GET": 0},
			)
			if err != nil {
				return fmt.Errorf("buildOptions for set operation: %v", err)
			}
		}
		err = handleSet(conn, requestArray[1], requestArray[2], options)
		if err != nil {
			return fmt.Errorf("handleSet: %v", err)
		}

	case "GET":
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

func handleSet(conn *Connection, key string, val string, options map[string][]string) error {
	now := time.Now().UnixMilli()

	entry := entry{
		value:    val,
		expireAt: 0,
	}
	if ex, ok := options["PX"]; ok {
		millisec, err := strconv.ParseInt(ex[0], 10, 64)
		if err != nil {
			return fmt.Errorf("the given PX option value cannot be converted into int64: %v", err)
		}
		entry.expireAt = now + millisec

	}
	cache[key] = entry

	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func handleGet(conn *Connection, key string) error {
	ret := "$-1\r\n"
	if entry, ok := cache[key]; ok {
		if entry.expireAt == 0 || entry.expireAt >= time.Now().UnixMilli() {
			ret = fmt.Sprintf("$%v\r\n%v\r\n", len(entry.value), entry.value)
		}
	}

	_, err := conn.Write([]byte(ret))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}
