package config

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"

	"github.com/mazen160/go-random"
)

var (
	whitespace                = regexp.MustCompile("[\t ]+")
	replicationIdCharacterSet = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// Opts represets the config given by users.
type Opts struct {
	Port       int    `short:"p" long:"port" default:"6379" description:"port number to bind this server"`
	ReplicaOf  string `long:"replicaof" description:"<master host> <master port>"`
	Dir        string `long:"dir" description:"the path to the directory where the RDB file is stored (example: /tmp/redis-data)"`
	DbFilename string `long:"dbfilename" description:"the name of the RDB file (example: rdbfile)"`

	// The below are the read-only opts induced by the user-given config values.

	Role              string
	MasterIP          net.IP
	MasterPort        int
	ReplicationID     string
	ReplicationOffset int
}

// Evaluate processes the given parameters, validates them, and populates induced read-only options.
func (o *Opts) Evaluate() error {
	//
	// Validate ReplicatOf
	//

	o.Role = "master" // default
	rid, err := random.Random(40, replicationIdCharacterSet, true)
	if err != nil {
		return fmt.Errorf("random.Random failed: %v", err)
	}
	o.ReplicationID = rid

	if o.ReplicaOf != "" {
		tokens := whitespace.Split(o.ReplicaOf, -1)
		if len(tokens) != 2 {
			return fmt.Errorf("wrong param to replicaof: %s", o.ReplicaOf)
		}

		// With LookupIP, you can handle strings line 'localhost' as well.
		ip, err := net.DefaultResolver.LookupIP(context.Background(), "ip4", tokens[0])
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
		o.ReplicationID = "" // I am slave. I don't have a replicaton ID.
	}
	return nil
}
