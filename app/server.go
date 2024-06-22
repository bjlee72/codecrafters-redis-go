package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/protocol"
	"github.com/codecrafters-io/redis-starter-go/storage"
	"github.com/jessevdk/go-flags"
)

func main() {
	var opts struct {
		// Slice of bool will append 'true' each time the option
		// is encountered (can be set multiple times, like -vvv)
		Port int `short:"p" long:"port" default:"6379" description:"Port number to start the process"`
	}

	_, err := flags.Parse(&opts)
	if err != nil {
		// We failed to parse the input parameter.
		panic(err)
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
			protocol.NewConnection(c),
			storage.GetCache())

		// fork goroutine
		go handler.Handle()
	}
}
