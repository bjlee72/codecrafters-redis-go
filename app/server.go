package main

import (
	"fmt"
	"log"
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

	if opts.Role != "master" {
		go connectMaster(opts)
	}

	runServer(opts)
}

func connectMaster(opts config.Opts) {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", opts.MasterIP, opts.MasterPort))
	if err != nil {
		log.Fatalf("net.Dial failed: %v", err)
	}

	handler := protocol.NewHandler(
		protocol.NewConnection(c),
		&opts,
		storage.GetCache(),
		nil,
	)

	if err = handler.Sync(); err != nil {
		log.Fatalf("handler.Sync failed: %v", err)
	}
}

func runServer(opts config.Opts) {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", opts.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	slaves := make(map[string]*protocol.Connection)

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		handler := protocol.NewHandler(
			protocol.NewConnection(c),
			&opts,
			storage.GetCache(),
			slaves,
		)

		go handler.Handle()
	}
}
