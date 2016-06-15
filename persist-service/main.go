package main

import (
	"flag"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "conf", "/etc/prestobloomfilterpersist.json", "Path to configuration JSON")
	flag.Parse()
}

func main() {
	// Config
	conf := newConf(configPath)

	// server
	server := newServer(conf)
	server.Start()
}
