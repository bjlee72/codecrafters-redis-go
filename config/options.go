package config

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
)

var (
	whitespace = regexp.MustCompile("[\t ]+")
)

// Opts represets the config given by users.
type Opts struct {
	Port      int    `short:"p" long:"port" default:"6379" description:"port number to bind this server"`
	ReplicaOf string `long:"replicaof" description:"<master host> <master port>"`

	// The below are the read-only opts induced by the user-given config values.

	Role       string
	MasterIP   net.IP
	MasterPort int
}

func (o *Opts) Validate() error {
	// Validate ReplicatOf
	o.Role = "master" // default
	if o.ReplicaOf != "" {
		tokens := whitespace.Split(o.ReplicaOf, -1)
		if len(tokens) != 2 {
			return fmt.Errorf("wrong param to replicaof: %s", o.ReplicaOf)
		}

		// With LookupIP, you can handle strings line 'localhost' as well.
		ip, err := net.LookupIP(tokens[0])
		if err != nil {
			return fmt.Errorf("not the valid IP address format: %s", tokens[0])
		}
		o.MasterIP = ip[0]

		port, err := strconv.Atoi(tokens[1])
		if err != nil {
			return fmt.Errorf("not the valid port number: %v", err)
		}
		o.MasterPort = port
		o.Role = "slave"
	}
	return nil
}
