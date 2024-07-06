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

type SlaveCallback func(mc *MasterConfig, s *Slave)

func (mc *MasterConfig) ForEachSlave(f SlaveCallback) {
	mc.slavesLock.RLock()
	{
		for _, s := range mc.slaves {
			f(mc, s)
		}
	}
	mc.slavesLock.RUnlock()
}

func (mc *MasterConfig) ForSlave(conn *Connection, f SlaveCallback) {
	mc.slavesLock.Lock()
	{
		remoteAddr := conn.conn.RemoteAddr().String()
		s, ok := mc.slaves[remoteAddr]
		if !ok {
			log.Fatal("cannot find the right slave: ", remoteAddr)
		}
		f(mc, s)
	}
	mc.slavesLock.Unlock()
}

func (mc *MasterConfig) AddSlave(conn *Connection) {
	remoteAddr := conn.RemoteAddr().String()
	s := NewSlave(conn)
	mc.slavesLock.Lock()
	{
		mc.slaves[remoteAddr] = s
	}
	mc.slavesLock.Unlock()
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
