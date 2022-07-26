package main

import (
	"flag"
	"fmt"
	"fractr-marketplace-secondary/match"
	"log"
	"net"

	"google.golang.org/grpc"

	msproto "github.com/blidd/fractr-proto/marketplace_secondary"
)

type Server struct {
	msproto.UnimplementedMarketplaceSecondaryServer
	match *match.OrderMatchingEngine
}

func New() *Server {

	server := &Server{
		match: match.New(),
	}

	go func(server *Server) {
		log.Printf("spinning up worker routine")
		for {
			select {
			case order := <-server.match.Orders():
				fmt.Printf("%+v\n", order)
			}
		}
	}(server)

	return server
}

var (
	port = flag.Int("port", 8080, "Server port")
)

func main() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	msproto.RegisterMarketplaceSecondaryServer(
		s,
		New(),
	)
	log.Printf("server listening at %v", listener.Addr())
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
