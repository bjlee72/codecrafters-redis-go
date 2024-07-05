package protocol

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/config"
	"github.com/codecrafters-io/redis-starter-go/info"
	"github.com/codecrafters-io/redis-starter-go/storage"
)

type Handler struct {
	opts  *config.Opts
	conn  *Connection
	cache *storage.Cache
	info  info.Info
}

func NewHandler(opts *config.Opts, conn *Connection, cache *storage.Cache) *Handler {
	return &Handler{
		opts:  opts,
		conn:  conn,
		cache: cache,
		info: info.Info{
			Replication: info.Replication{
				Role:             opts.Role,
				MasterReplID:     opts.ReplicationID,
				MasterReplOffset: opts.ReplicationOffset,
			},
		},
	}
}

// Sync syncs the status of cache with master.
func (h *Handler) Sync() error {
	defer h.conn.Close()

	/*
	 * Handshake process.
	 */

	if err := h.conn.Write("*1\r\n$4\r\nPING\r\n"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	if _, err := h.shouldRead("PONG"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	portStr := strconv.Itoa(h.opts.Port)
	if err := h.conn.Write(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(portStr), portStr)); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	if _, err := h.shouldRead("OK"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	if err := h.conn.Write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	if _, err := h.shouldRead("OK"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	if err := h.conn.Write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	if _, err := h.shouldReadPrefix("FULLRESYNC"); err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}

	return nil
}

func (h *Handler) Handle() {
	defer h.conn.Close()

	// read, validate, and process.

	for {
		request, err := h.read()
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return
		}

		// requestArray is a single request from a client.
		if err := h.processRequest(request); err != nil {
			fmt.Fprintf(os.Stderr, "handleRequest failed: %v", err)
			return
		}
	}
}

func (h *Handler) read() ([]string, error) {
	token, err := h.conn.Read()
	if err != nil {
		return nil, fmt.Errorf("conn.Read(): %v", err)
	}

	if token[0] == '+' { // simple string
		return []string{token[1:]}, nil
	}

	num, err := ValidateArray(token)
	if err != nil {
		return nil, fmt.Errorf("ValidateArray(): %v", err)
	}

	requestArray := make([]string, 0, num)
	for i := 0; i < num; i++ {
		token, err := h.conn.Read()
		if err != nil {
			return nil, fmt.Errorf("h.conn.GetToken(): %v", err)
		}

		l, err := ValidateBulkString(token)
		if err != nil {
			return nil, fmt.Errorf("ValidateString(): %v", err)
		}

		str, err := h.conn.Read()
		if err != nil {
			return nil, fmt.Errorf("h.conn.GetToken(): %v", err)
		}
		if l != len(str) {
			return nil, fmt.Errorf("bulk string length mismatch: %v != %v", l, len(str))
		}

		requestArray = append(requestArray, str)
	}

	return requestArray, nil
}

func (h *Handler) shouldRead(cmd string) ([]string, error) {
	msg, err := h.read()
	if err != nil {
		return nil, fmt.Errorf("h.read failed: %v", err)
	}

	if !strings.EqualFold(cmd, msg[0]) {
		return nil, fmt.Errorf("cmd mismatch: expected: %s actual: %v", cmd, msg)
	}

	return msg, nil
}

func (h *Handler) shouldReadPrefix(cmd string) ([]string, error) {
	msg, err := h.read()
	if err != nil {
		return nil, fmt.Errorf("h.read failed: %v", err)
	}

	prefix := msg[0][:len(cmd)]
	if !strings.EqualFold(cmd, prefix) {
		return nil, fmt.Errorf("cmd mismatch: expected: %s actual: %v", cmd, msg)
	}

	return msg, nil
}

func (h *Handler) processRequest(requestArray []string) error {
	cmd := strings.ToUpper(requestArray[0])

	switch cmd {

	case "PING":
		err := h.handlePing()
		if err != nil {
			return fmt.Errorf("handlePing: %v", err)
		}

	case "ECHO":
		err := h.handleEcho(requestArray[1])
		if err != nil {
			return fmt.Errorf("handleEcho: %v", err)
		}

	case "INFO":
		err := h.handleInfo(requestArray[1:])
		if err != nil {
			return fmt.Errorf("handleInfo: %v", err)
		}

	case "SET":
		options := map[string][]string{}
		var err error
		if len(requestArray) > 3 {
			options, err = BuildOptions(
				requestArray[3:],
				OptionConfig{"EX": 1, "PX": 1, "EXAT": 1, "PXAT": 1, "NX": 0, "XX": 0, "KEEPTTL": 0, "GET": 0},
			)
			if err != nil {
				return fmt.Errorf("buildOptions for set operation: %v", err)
			}
		}
		err = h.handleSet(requestArray[1], requestArray[2], options)
		if err != nil {
			return fmt.Errorf("handleSet: %v", err)
		}

	case "GET":
		err := h.handleGet(requestArray[1])
		if err != nil {
			return fmt.Errorf("handleGet: %v", err)
		}

	case "REPLCONF":
		err := h.handleReplConf(requestArray[1:])
		if err != nil {
			return fmt.Errorf("handleReplConf: %v", err)
		}
	}

	return nil
}

func (h *Handler) handlePing() error {
	err := h.conn.Write("+PONG\r\n")
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleEcho(val string) error {
	ret := fmt.Sprintf("$%v\r\n%v\r\n", len(val), val)

	err := h.conn.Write(ret)
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleSet(key, val string, options map[string][]string) error {
	var expireAfter int64

	if ex, ok := options["PX"]; ok {
		millisec, err := strconv.ParseInt(ex[0], 10, 64)
		if err != nil {
			return fmt.Errorf("the given PX option value cannot be converted into int64: %v", err)
		}
		expireAfter = millisec
	}

	h.cache.Set(key, val, expireAfter)

	err := h.conn.Write("+OK\r\n")
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleGet(key string) error {
	ret := "$-1\r\n"

	if entry, err := h.cache.Get(key); entry != nil && err == nil {
		ret = fmt.Sprintf("$%v\r\n%v\r\n", len(*entry), *entry)
	}

	err := h.conn.Write(ret)
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleInfo(_ []string) error {
	ret := "$-1\r\n"

	if str, err := h.info.ToRedisBulkString(); err == nil {
		ret = str
	}

	err := h.conn.Write(ret)
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleReplConf(_ []string) error {
	ret := "+OK\r\n"

	err := h.conn.Write(ret)
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}
