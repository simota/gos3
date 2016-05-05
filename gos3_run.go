package main

import (
	"flag"

	"./gos3"
)

func main() {
	var (
		port  string
		fast  bool
		debug bool
	)
	flag.StringVar(&port, "p", "4567", "port")
	flag.BoolVar(&debug, "debug", false, "debug mode")
	flag.BoolVar(&fast, "fast", false, "use fasthttp")
	flag.Parse()

	gos3.Run(":"+port, fast, debug)
}
