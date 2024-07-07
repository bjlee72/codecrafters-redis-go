package protocol

import (
	"log"
	"sync"
)

// MasterConfig is a config shared by all Master handlers.
type MasterConfig struct {
	slaves            map[string]*Slave
	slavesLock        sync.RWMutex
	propagationOffset uint64 // the offset that we expect to be acknowledged by the next REPLCONF ACK ?? response.
	wg                *sync.WaitGroup
}

func NewMasterConfig() *MasterConfig {
	return &MasterConfig{
		slaves:     make(map[string]*Slave, 0),
		slavesLock: sync.RWMutex{},
		wg:         &sync.WaitGroup{},
	}
}

func (mc *MasterConfig) SyncedSlaveNum() int {
	mc.slavesLock.RLock()
	defer mc.slavesLock.RUnlock()

	var result int
	for _, s := range mc.slaves {
		if mc.propagationOffset == s.propagatedOffset {
			result += 1
		}
	}

	return result
}

func (mc *MasterConfig) AckSlave(conn *Connection, offset uint64) {
	mc.slavesLock.Lock()
	defer mc.slavesLock.Unlock()

	remoteAddr := conn.conn.RemoteAddr().String()
	s, ok := mc.slaves[remoteAddr]
	if !ok {
		log.Fatal("cannot find the right slave: ", remoteAddr)
	}

	s.propagatedOffset = offset
	if s.propagatedOffset == mc.propagationOffset {
		mc.wg.Done()
	}
}

func (mc *MasterConfig) AddSlave(conn *Connection) {
	mc.slavesLock.Lock()
	defer mc.slavesLock.Unlock()

	mc.slaves[conn.RemoteAddr().String()] = NewSlave(conn)
}

type Slave struct {
	conn             *Connection
	propagatedOffset uint64 // the offset that the slave last acked with REPLCONF ACK ?? response.
}

func NewSlave(conn *Connection) *Slave {
	return &Slave{
		conn: conn,
	}
}
