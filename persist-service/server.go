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
	backend        IBackend
}

// Init
func (s *Server) init() {
	// Backend
	s.backend = newBackend(s.conf)
	if s.backend == nil {
		log.Fatal("Backend not initiated")
	}

	// Router
	s.router = routing.New()

	// Upsert operation that writes data
	s.router.Put("/bloomfilter/<key>", func(c *routing.Context) error {
		// Params
		key := c.Param("key")
		body := c.PostBody()

		// Put
		res, resErr := s.backend.Put([]byte(key), body)

		// Log
		log.Printf("PUT %v %d %v %v", key, len(body), res, resErr)
		return nil
	})

	// Get
	s.router.Get("/bloomfilter/<key>", func(c *routing.Context) error {
		// Params
		key := c.Param("key")

		// Get
		res, resErr := s.backend.Get([]byte(key))

		// Output
		fmt.Fprintf(c, "%s", res)

		// Log
		log.Printf("GET %v %d %v", key, len(res), resErr)
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
