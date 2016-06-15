package main

import (
	"fmt"
	"log"

	"github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
)

type Server struct {
	requestHandler func(ctx *fasthttp.RequestCtx)
	conf           *Conf
	router         *routing.Router
}

// Init
func (s *Server) init() {
	s.router = routing.New()
	s.router.Post("/bloomfilter/<key>", func(c *routing.Context) error {
		// Params
		key := c.Param("key")
		body := c.PostBody()

		log.Printf("%v %v", key, body)
		return nil
	})
}

// Start
func (s *Server) Start() {
	listenStr := fmt.Sprintf("%s:%d", s.conf.ListenHost, s.conf.ListenPort)
	log.Printf("Starting server at %s", listenStr)
	panic(fasthttp.ListenAndServe(listenStr, s.router.HandleRequest))
}

// Constructor
func newServer(conf *Conf) *Server {
	s := &Server{
		conf: conf,
	}
	s.init()
	return s
}
