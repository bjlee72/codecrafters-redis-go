package protocol

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/config"
	"github.com/codecrafters-io/redis-starter-go/info"
	"github.com/codecrafters-io/redis-starter-go/storage"
)

type Handler struct {
	server bool
	opts   *config.Opts
	conn   *Connection
	cache  *storage.Cache
	info   info.Info

	// only for master.
	mc *MasterConfig

	// only for slaves.
	replicationStartOffset uint64
}

func NewClient(conn *Connection, opts *config.Opts, cache *storage.Cache) *Handler {
	return newHandler(conn, false, opts, cache, nil)
}

func NewServer(conn *Connection, opts *config.Opts, cache *storage.Cache, mc *MasterConfig) *Handler {
	return newHandler(conn, true, opts, cache, mc)
}

func newHandler(conn *Connection, server bool, opts *config.Opts, cache *storage.Cache, mc *MasterConfig) *Handler {
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
		replicationStartOffset: 0,
		mc:                     mc,
	}
}

func (h *Handler) Handle() error {
	defer h.conn.Close()

	if !h.server && h.opts.Role == "slave" {
		// I'm connecting master as slave. NOTE: slave can be a server as well (for example, for INFO command)
		if err := h.conn.Write(PING); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldReadReply("PONG"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		replConf1 := NewArray([]string{"REPLCONF", "listening-port", fmt.Sprintf("%d", h.opts.Port)})
		if err := h.conn.Write(replConf1); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldReadReply("OK"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		replConf2 := NewArray([]string{"REPLCONF", "capa", "psync2"})
		if err := h.conn.Write(replConf2); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldReadReply("OK"); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		psync := NewArray([]string{"PSYNC", "?", "-1"})
		if err := h.conn.Write(psync); err != nil {
			return fmt.Errorf("conn.Write failed: %w", err)
		}

		if _, err := h.shouldReadReply("FULLRESYNC"); err != nil {
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
			if err := h.propagate(request); err != nil {
				err = fmt.Errorf("h.propagate failed: %w", err)
				fmt.Fprintln(os.Stderr, err.Error())
				return err
			}
		}
	}
}

func (h *Handler) propagate(msg Message) error {
	if !msg.Propagatible() {
		return nil
	}

	h.mc.AdvancePropagation(len(msg.Redis()))

	errors := make([]string, 0)
	h.mc.ForEachSlave(func(mc *MasterConfig, s *Slave) {
		if err := s.conn.Write(msg); err != nil {
			errors = append(errors, fmt.Sprintf("conn.Write: %v", err))
		}
	})

	if len(errors) > 0 {
		return fmt.Errorf("propagate failed: [%s]", strings.Join(errors, ", "))
	}

	return nil
}

// read is a basic request-reading routine. Assumes the request is always an array.
func (h *Handler) read() (Message, error) {
	token, err := h.conn.Read()
	if err != nil {
		return nil, fmt.Errorf("conn.Read(): %w", err)
	}

	if token[0] == '+' { // simple string
		return NewSimple(token[1:]), nil
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

	return NewArray(requestArray), nil
}

func (h *Handler) shouldReadReply(prefix string) (Message, error) {
	msg, err := h.read()
	if err != nil {
		return nil, fmt.Errorf("h.read failed: %w", err)
	}

	var reply string
	switch v := msg.(type) {
	case *ArrayMessage:
		reply = v.Raw()[0]
	case *SimpleMessage:
		reply = v.Raw()
	case *BulkMessage:
		reply = v.Raw()
	default:
		// cannot understood.
		return nil, fmt.Errorf("cannot process reply: %v", msg)
	}

	if !strings.HasPrefix(strings.ToUpper(reply), strings.ToUpper(prefix)) {
		return nil, fmt.Errorf("cmd mismatch: expected: %s actual: %v", prefix, msg)
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
func (h *Handler) processRequest(request Message) error {
	msg, ok := request.(*ArrayMessage)
	if !ok {
		// TODO: request that cannot be understood by this logic.
		return fmt.Errorf("couldn't understand request: %v", request)
	}

	switch msg.Raw()[0] {
	case "CONFIG":
		err := h.handleConfig(msg.Token(1), msg.Token(2))
		if err != nil {
			return fmt.Errorf("handleConfig: %v", err)
		}

	case "PING":
		if h.server {
			// We don't handle ping when master sends to slave for keep-alive purpose.
			err := h.handlePing()
			if err != nil {
				return fmt.Errorf("handlePing: %v", err)
			}
		}

	case "ECHO":
		err := h.handleEcho(msg.Token(1))
		if err != nil {
			return fmt.Errorf("handleEcho: %v", err)
		}

	case "INFO":
		err := h.handleInfo()
		if err != nil {
			return fmt.Errorf("handleInfo: %v", err)
		}

	case "SET":
		options := map[string][]string{}
		var err error
		if msg.Len() > 3 {
			options, err = BuildOptions(
				msg.SliceFrom(3),
				OptionConfig{"EX": 1, "PX": 1, "EXAT": 1, "PXAT": 1, "NX": 0, "XX": 0, "KEEPTTL": 0, "GET": 0},
			)
			if err != nil {
				return fmt.Errorf("buildOptions for set operation: %v", err)
			}
		}

		key, value := msg.Token(1), msg.Token(2)
		err = h.handleSet(key, value, options)
		if err != nil {
			return fmt.Errorf("handleSet: %v", err)
		}

	case "GET":
		err := h.handleGet(msg.Token(1))
		if err != nil {
			return fmt.Errorf("handleGet: %v", err)
		}

	case "REPLCONF":
		err := h.handleReplConf(msg.SliceFrom(1))
		if err != nil {
			return fmt.Errorf("handleReplConf: %v", err)
		}

	case "PSYNC":
		if h.opts.Role != "master" {
			return fmt.Errorf("role is not master: %s", h.opts.Role)
		}

		// register a new slave to update continuously.
		h.mc.AddSlave(h.conn)

		offset, err := strconv.Atoi(msg.Token(2))
		if err != nil {
			return fmt.Errorf("strconv.Atoi: %w", err)
		}

		err = h.handlePsync(msg.Token(1), offset)
		if err != nil {
			return fmt.Errorf("handlePsync: %w", err)
		}

	case "WAIT":
		if h.opts.Role != "master" {
			return fmt.Errorf("role is not master: %s", h.opts.Role)
		}

		numReplicas, err := strconv.Atoi(msg.Token(1))
		if err != nil {
			return fmt.Errorf("strconv.Atoi: %w", err)
		}

		timeout, err := strconv.Atoi(msg.Token(2))
		if err != nil {
			return fmt.Errorf("strconv.Atoi: %w", err)
		}

		err = h.handleWait(numReplicas, timeout)
		if err != nil {
			return fmt.Errorf("h.handleWait failed: %w", err)
		}
	}

	return nil
}

func (h *Handler) handleConfig(cmd, param string) error {
	if strings.EqualFold(cmd, "GET") {
		if strings.EqualFold(param, "dir") {
			if err := h.conn.Write(NewArray([]string{"dir", h.opts.Dir})); err != nil {
				return fmt.Errorf("h.conn.Write failed: %w", err)
			}
		} else if strings.EqualFold(param, "dir") {
			if err := h.conn.Write(NewArray([]string{"dbfilename", h.opts.DbFilename})); err != nil {
				return fmt.Errorf("h.conn.Write failed: %w", err)
			}
		}
	}
	return fmt.Errorf("cannot process the cmd: %s %s", cmd, param)
}

func (h *Handler) handlePing() error {
	if err := h.conn.Write(PONG); err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleEcho(val string) error {
	echo := NewBulk(val)

	if err := h.conn.Write(echo); err != nil {
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

	err := h.conn.Write(OK)
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}

func (h *Handler) handleGet(key string) error {
	var cachedValue Message = NULL

	if entry, err := h.cache.Get(key); entry != nil && err == nil {
		cachedValue = NewBulk(*entry)
	}

	err := h.conn.Write(cachedValue)
	if err != nil {
		return fmt.Errorf("write response failed: %w", err)
	}

	return nil
}

func (h *Handler) handleInfo() error {
	info := NewBulk(strings.Join(h.info.Info(), "\r\n"))

	err := h.conn.Write(info)
	if err != nil {
		return fmt.Errorf("write response failed: %w", err)
	}

	return nil
}

func (h *Handler) handleWait(numReplicas, timeout int) error {
	toWait := make([]*Connection, 0)
	for _, slave := range h.mc.slaves {
		if h.mc.propagationOffset > slave.propagatedOffset {
			toWait = append(toWait, slave.conn)
		}
	}
	if len(toWait) == 0 {
		// nothing to wait for - return immediately.
		allInSync := NewInt(len(h.mc.slaves))
		err := h.conn.Write(allInSync)
		if err != nil {
			return fmt.Errorf("h.conn.Write failed: %w", err)
		}
		return nil
	}

	slaveAckWG := h.mc.NewSlaveAckWG(min(len(toWait), numReplicas))

	getAck := NewArray([]string{"REPLCONF", "GETACK", "*"})
	for _, c := range toWait {
		// cannot use h.conn.Write here because we should send to slave
		if err := c.Write(getAck); err != nil {
			fmt.Fprintf(os.Stderr, "c.Write failed: %v", err)
			slaveAckWG.Done()
		}
	}

	slaveAckWG.TimedWait(timeout)

	syncedSlaves := NewInt(h.mc.SyncedSlaveNum())
	if err := h.conn.Write(syncedSlaves); err != nil {
		return fmt.Errorf("h.conn.Write failed: %w", err)
	}

	// because we sent getAck command for this
	h.mc.AdvancePropagation(len(getAck.Redis()))

	return nil
}

func (h *Handler) handleReplConf(request []string) error {
	if h.opts.Role != "master" && CommandEquals(request[0], "GETACK") {
		// 37 represents the length of REPLCONF GETACK * command itself.
		transferred := h.conn.Offset() - h.replicationStartOffset - 37
		r := strconv.FormatUint(transferred, 10)

		// send response to master.
		if err := h.conn.Write(NewArray([]string{"REPLCONF", "ACK", r})); err != nil {
			return fmt.Errorf("h.conn.Write failed: %w", err)
		}

		return nil

	} else if h.opts.Role == "master" && CommandEquals(request[0], "ACK") {
		offset, err := strconv.ParseUint(request[1], 10, 64)
		if err != nil {
			return fmt.Errorf("strconf.ParseInt failed: %w", err)
		}

		h.mc.AckSlave(h.conn, offset)

		return nil
	}

	err := h.conn.Write(OK)
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
		fullResync := NewSimple(fmt.Sprintf("FULLRESYNC %s 0", h.opts.ReplicationID))
		if err := h.conn.Write(fullResync); err != nil {
			return fmt.Errorf("write response failed: %w", err)
		}

		rdb, err := h.readRDB()
		if err != nil {
			return fmt.Errorf("readRDB failed: %w", err)
		}

		// RDB has a weird ending rule (not ends with \r\n), so we don't usage h.conn.Write here.
		rdb = fmt.Sprintf("$%d\r\n%v", len(rdb), rdb)
		if err := h.conn.WriteString(rdb); err != nil {
			return fmt.Errorf("write response failed: %w", err)
		}
	} else {
		return fmt.Errorf("not implemented")
		// Should implement a response to PSYNC <replicationid> <offset>
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
