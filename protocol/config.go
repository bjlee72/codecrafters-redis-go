package protocol

import (
	"log"
	"sync"
	"time"
)

type SlaveAckWG struct {
	cnt int
	wg  *sync.WaitGroup
}

func NewSlaveAckWG(howmany int) *SlaveAckWG {
	wg := &sync.WaitGroup{}
	wg.Add(howmany)

	return &SlaveAckWG{
		cnt: howmany,
		wg:  wg,
	}
}

func (saw *SlaveAckWG) Done() bool {
	saw.wg.Done()
	saw.cnt--

	return saw.cnt == 0
}

func (saw *SlaveAckWG) TimedWait(timeoutMillis int) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		saw.wg.Wait()
	}()

	select {
	case <-c:
	case <-time.After(time.Millisecond * time.Duration(timeoutMillis)):
	}
}

// MasterConfig is a config shared by all Master handlers.
type MasterConfig struct {
	slaves            map[string]*Slave
	slavesLock        sync.RWMutex
	propagationOffset uint64 // the offset that we expect to be acknowledged by the next REPLCONF ACK ?? response.
	slaveAckWGs       []*SlaveAckWG
}

func NewMasterConfig() *MasterConfig {
	return &MasterConfig{
		slaves:     make(map[string]*Slave, 0),
		slavesLock: sync.RWMutex{},
	}
}

func (mc *MasterConfig) ForEachSlave(f func(mc *MasterConfig, s *Slave)) {
	for _, s := range mc.slaves {
		f(mc, s)
	}
}

func (mc *MasterConfig) AdvancePropagation(amount int) {
	mc.propagationOffset += uint64(amount)
}

func (mc *MasterConfig) NewSlaveAckWG(howmany int) *SlaveAckWG {
	mc.slavesLock.Lock()
	defer mc.slavesLock.Unlock()

	new := NewSlaveAckWG(howmany)
	mc.slaveAckWGs = append(mc.slaveAckWGs, new)

	return new
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

	front := mc.slaveAckWGs[0]
	if front.Done() {
		mc.slaveAckWGs = mc.slaveAckWGs[1:]
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
