package main

import (
	"flag"
	"log"

	"github.com/dustin/gomemcached/client"
	"github.com/dustin/gotap"
)

var prot = flag.String("prot", "tcp", "Layer 3 protocol (tcp, tcp4, tcp6)")
var dest = flag.String("dest", "localhost:11211", "Host:port to connect to")
var u = flag.String("user", "", "SASL plain username")
var p = flag.String("pass", "", "SASL plain password")

func main() {
	flag.Parse()
	log.Printf("Connecting to %s/%s", *prot, *dest)

	client, err := memcached.Connect(*prot, *dest)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}

	if *u != "" {
		_, err := client.Auth(*u, *p)
		if err != nil {
			log.Fatalf("auth error: %v", err)
		}
	}

	args := tap.TapArguments{
		Backfill:   131313,
		VBuckets:   []uint16{0, 2, 4},
		ClientName: "go_go_gadget_tap",
	}

	ch, err := tap.Feed(client, args)
	if err != nil {
		log.Fatalf("Error starting tap feed: %v", err)
	}
	for op := range ch {
		log.Printf("Tap OP:  %s\n", op.ToString())
	}
}
