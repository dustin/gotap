package main

import (
	"flag"
	"github.com/dustin/gotap"
	"log"
)

var prot = flag.String("prot", "tcp", "Layer 3 protocol (tcp, tcp4, tcp6)")
var dest = flag.String("dest", "localhost:11211", "Host:port to connect to")
var u = flag.String("user", "", "SASL plain username")
var p = flag.String("pass", "", "SASL plain password")

func main() {
	flag.Parse()
	log.Printf("Connecting to %s/%s", *prot, *dest)

	client := tap.Connect(*prot, *dest)

	if *u != "" {
		err := client.AuthPlain(*u, *p)
		if err != nil {
			log.Fatalf("auth error: %v", err)
		}
	}

	var args tap.TapArguments
	args.Backfill = 131313
	args.VBuckets = []uint16{0, 2, 4}
	args.ClientName = "go_go_gadget_tap"
	client.Start(args)

	for op := range client.Feed() {
		log.Printf("Tap OP:  %s\n", op.ToString())
	}
}
