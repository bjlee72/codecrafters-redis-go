package protocol

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/config"
	"github.com/codecrafters-io/redis-starter-go/info"
	"github.com/codecrafters-io/redis-starter-go/storage"
)

type Handler struct {
	server                 bool
	opts                   *config.Opts
	conn                   *Connection
	cache                  *storage.Cache
	info                   info.Info
	slaves                 map[string]*Connection
	slavesLock             sync.RWMutex
	replicationStartOffset uint64
}

func NewClient(conn *Connection, opts *config.Opts, cache *storage.Cache, slaves map[string]*Connection) *Handler {
	return newHandler(conn, false, opts, cache, slaves)
}

func NewServer(conn *Connection, opts *config.Opts, cache *storage.Cache, slaves map[string]*Connection) *Handler {
	return newHandler(conn, true, opts, cache, slaves)
}

func newHandler(conn *Connection, server bool, opts *config.Opts, cache *storage.Cache, slaves map[string]*Connection) *Handler {
	return &Handler{
		conn:   conn,
		server: server,
		opts:   opts,
		cache:  cache,
		info: info.Info{
			Replication: info.Replication{
				Role:             opts.Role,
				MasterReplID:     opts.ReplicationID,
				MasterReplOffset: opts.ReplicationOffset,
			},
		},
		slaves:                 slaves,
		replicationStartOffset: 0,
	}
}

func (h *Handler) Handle() error {
	defer h.conn.Close()

	if !h.server && h.opts.Role == "slave" {
		// I'm connecting master as slave. NOTE: slave can be a server as well (for example, for INFO command)
		if err := h.conn.Write("*1\r\n$4\r\nPING\r\n"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldRead("PONG"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		portStr := strconv.Itoa(h.opts.Port)
		if err := h.conn.Write(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(portStr), portStr)); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldRead("OK"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if err := h.conn.Write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldRead("OK"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if err := h.conn.Write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldReadPrefix("FULLRESYNC"); err != nil {
			return fmt.Errorf("shouldReadPrefix failed: %w", err)
		}

		if err := h.shouldReadRDB(); err != nil {
			return fmt.Errorf("shouldReadRDB: %w", err)
		}

		h.replicationStartOffset = h.conn.Offset()
	}

	for {
		request, err := h.read()
		if err != nil {
			err = fmt.Errorf("h.read failed: %w", err)
			fmt.Fprintln(os.Stderr, err.Error())
			return err
		}

		// requestArray is a single request from a client.
		err = h.processRequest(request)
		if err != nil {
			err = fmt.Errorf("handleRequest failed: %w", err)
			fmt.Fprintln(os.Stderr, err.Error())
			return err
		}

		if h.opts.Role == "master" {
			if err := h.propagate(request[0], ToBulkStringArray(request)); err != nil {
				err = fmt.Errorf("h.propagate failed: %w", err)
				fmt.Fprintln(os.Stderr, err.Error())
				return err
			}
		}
	}
}

var propagateCommand = map[string]bool{
	"SET": true,
}

func (h *Handler) propagate(cmd string, bulkArray string) error {
	if !propagateCommand[cmd] {
		return nil
	}

	errors := make([]string, 0)
	for _, conn := range h.slaves {
		if err := conn.Write(bulkArray); err != nil {
			errors = append(errors, fmt.Sprintf("conn.Write: %v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("propagate failed: [%s]", strings.Join(errors, ", "))
	}

	return nil
}

// read is a basic request-reading routine. Assumes the request is always an array.
func (h *Handler) read() ([]string, error) {
	token, err := h.conn.Read()
	if err != nil {
		return nil, fmt.Errorf("conn.Read(): %w", err)
	}

	if token[0] == '+' { // simple string
		return []string{token[1:]}, nil
	}

	num, err := ArrayLength(token)
	if err != nil {
		return nil, fmt.Errorf("ValidateArray(): %w", err)
	}

	requestArray := make([]string, 0, num)
	for i := 0; i < num; i++ {
		token, err := h.conn.Read()
		if err != nil {
			return nil, fmt.Errorf("h.conn.GetToken(): %w", err)
		}

		l, err := BulkStringLength(token)
		if err != nil {
			return nil, fmt.Errorf("ValidateString(): %w", err)
		}

		str, err := h.conn.Read()
		if err != nil {
			return nil, fmt.Errorf("h.conn.GetToken(): %w", err)
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
		return nil, fmt.Errorf("h.read failed: %w", err)
	}

	if !strings.EqualFold(cmd, msg[0]) {
		return nil, fmt.Errorf("cmd mismatch: expected: %s actual: %v", cmd, msg)
	}

	return msg, nil
}

func (h *Handler) shouldReadPrefix(cmd string) ([]string, error) {
	msg, err := h.read()
	if err != nil {
		return nil, fmt.Errorf("h.read failed: %w", err)
	}

	prefix := msg[0][:len(cmd)]
	if !strings.EqualFold(cmd, prefix) {
		return nil, fmt.Errorf("cmd mismatch: expected: %s actual: %v", cmd, msg)
	}

	return msg, nil
}

func (h *Handler) shouldReadRDB() error {
	typ, err := h.conn.Read()
	if err != nil {
		return fmt.Errorf("h.conn.Read: %w", err)
	}

	if typ[0] != '$' {
		return fmt.Errorf("unexpected type than $: %v", typ[0])
	}

	total, err := strconv.Atoi(typ[1:])
	if err != nil {
		return fmt.Errorf("strconf.Atoi: %w", err)
	}

	result := make([]byte, 0, total)
	for total > 0 {
		tmp := make([]byte, min(1024, total))
		rd, err := h.conn.ReadBytes(tmp)
		if err != nil {
			if err == io.EOF {
				total = total - rd
				break
			} else {
				return fmt.Errorf("h.conn.ReadBytes: %w", err)
			}
		}

		if rd == 0 {
			continue
		}

		total = total - rd

		// TODO: copy buf to somewhere.
		result = append(result, tmp[:rd]...)
	}

	_ = result

	if total > 0 {
		// incomplete termination.
		return fmt.Errorf("couldn't read RDB fully")
	}

	return nil
}

// the first return arg is a flag for propagation to secondaries.
func (h *Handler) processRequest(requestArray []string) error {
	cmd := strings.ToUpper(requestArray[0])

	switch cmd {

	case "PING":
		if h.server {
			// We don't handle ping when master sends to slave for keep-alive purpose.
			err := h.handlePing()
			if err != nil {
				return fmt.Errorf("handlePing: %v", err)
			}
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

		key, value := requestArray[1], requestArray[2]
		err = h.handleSet(key, value, options)
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

	case "PSYNC":
		if h.opts.Role != "master" {
			return fmt.Errorf("role is not master: %s", h.opts.Role)
		}

		offset, err := strconv.Atoi(requestArray[2])
		if err != nil {
			return fmt.Errorf("strconv.Atoi: %v", err)
		}

		err = h.handlePsync(requestArray[1], offset)
		if err != nil {
			return fmt.Errorf("handlePsync: %v", err)
		}

		// register a new slave to update continuously.
		h.slavesLock.Lock()
		remoteAddr := h.conn.RemoteAddr().String()
		h.slaves[remoteAddr] = h.conn
		h.slavesLock.Unlock()
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

	if h.opts.Role != "master" {
		return nil
	}

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
		return fmt.Errorf("write response failed: %w", err)
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
		return fmt.Errorf("write response failed: %w", err)
	}

	return nil
}

func (h *Handler) handleReplConf(request []string) error {
	if h.opts.Role != "master" {
		if !strings.EqualFold(request[0], "GETACK") {
			return fmt.Errorf("cannot handle command: %v", request)
		}

		transferred := h.conn.Offset() - h.replicationStartOffset - 37
		r := strconv.FormatUint(transferred, 10)

		err := h.conn.Write(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(r), r))
		if err != nil {
			return fmt.Errorf("h.conn.Write failed: %w", err)
		}

		return nil
	}

	ret := "+OK\r\n"

	err := h.conn.Write(ret)
	if err != nil {
		return fmt.Errorf("write response failed: %w", err)
	}

	return nil
}

func (h *Handler) handlePsync(id string, offset int) error {
	// replication id and offset is not used for now.
	if id != "?" && id != h.opts.ReplicationID {
		return fmt.Errorf("client-sent rid is different from ours: client: %s, this: %s", id, h.opts.ReplicationID)
	}

	if id == "?" && offset > 0 {
		return fmt.Errorf("wrong rsync request: id: %s, offset: %d", id, offset)
	}

	if offset <= 0 { // FULLRESYNC
		ret := fmt.Sprintf("+FULLRESYNC %s 0\r\n", h.opts.ReplicationID)
		if err := h.conn.Write(ret); err != nil {
			return fmt.Errorf("write response failed: %w", err)
		}

		rdb, err := h.readRDB()
		if err != nil {
			return fmt.Errorf("readRDB failed: %w", err)
		}

		ret = fmt.Sprintf("$%d\r\n%v", len(rdb), rdb)

		// send content of the RDB file.
		if err := h.conn.Write(ret); err != nil {
			return fmt.Errorf("write response failed: %w", err)
		}
	} else {
		return fmt.Errorf("not implemented")
	}

	return nil
}

// readRDB returns the base64-decoded RDB file.
func (h *Handler) readRDB() (string, error) {
	var (
		b64 = `UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==`
	)

	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", fmt.Errorf("base64.StdEncoding.DecodeString: %w", err)
	}

	return string(decoded), nil
}
