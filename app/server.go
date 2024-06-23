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

		// fork goroutine
		go handler.Handle()
	}
}
