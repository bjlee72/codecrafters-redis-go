package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/config"
	"github.com/codecrafters-io/redis-starter-go/protocol"
	"github.com/codecrafters-io/redis-starter-go/storage"
	"github.com/jessevdk/go-flags"
)

func main() {
	var opts config.Opts

	_, err := flags.Parse(&opts)
	if err != nil {
		// We failed to parse the input parameter.
		panic(err)
	}

	err = opts.Validate()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if opts.Role == "slave" {
		err := handshake(opts)
		if err != nil {
			fmt.Println("handshake with master failed: ", err.Error())
			os.Exit(1)
		}
	}

	runServer(opts)
}

func handshake(opts config.Opts) error {
	ip, port := opts.MasterIP, opts.MasterPort

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return fmt.Errorf("net.Dial failed: %v", err)
	}

	conn := protocol.NewConnection(c)
	err = conn.Write("*1\r\n$4\r\nPING\r\n")
	if err != nil {
		return fmt.Errorf("conn.Write failed: %v", err)
	}
	return nil
}

func runServer(opts config.Opts) {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", opts.Port))
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

		handler := protocol.NewHandler(
			&opts,
			protocol.NewConnection(c),
			storage.GetCache())

		go handler.Handle()
	}
}
