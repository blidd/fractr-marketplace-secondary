package main

import (
	"flag"
	"fmt"
	"fractr-marketplace-secondary/libstore"
	"fractr-marketplace-secondary/match"
	"fractr-marketplace-secondary/pqueue"
	"log"
	"net"

	"google.golang.org/grpc"

	mcproto "github.com/blidd/fractr-proto/marketplace_common"
	msproto "github.com/blidd/fractr-proto/marketplace_secondary"
	"github.com/blidd/fractr-proto/storage"
)

type Server struct {
	msproto.UnimplementedMarketplaceSecondaryServer
	match *match.OrderMatchingEngine
	ls    *libstore.Libstore
}

func New() *Server {

	server := &Server{
		match: match.New(),
		ls:    libstore.NewLibstore(string(fmt.Sprintf("[::1]:%d", *storageServicePort))),
	}

	go func(server *Server) {
		log.Printf("spinning up worker routine")
		for {
			select {
			case tx := <-server.match.Orders():
				fmt.Printf("%+v\n", tx)
				// send order to smart contract for execution

			case order := <-server.match.Jobs():

				var status mcproto.Status
				if order.QuantityFilled() == order.Quantity() {
					status = mcproto.Status_COMPLETE
				} else if order.QuantityFilled() > 0 {
					status = mcproto.Status_PARTIALLY_FILLED
				} else {
					status = mcproto.Status_NEW
				}

				switch ord := order.(type) {
				case *pqueue.Bid:
					bidUpdate := &mcproto.BidStatus{
						Bid: &mcproto.Bid{
							Id:        ord.Id,
							ArtworkId: ord.ArtworkId,
							BidderId:  ord.BidderId,
							Quantity:  ord.Quantity(),
							Price:     ord.Price,
						},
						QuantityFilled: ord.QuantityFilled(),
						Status:         status,
					}
					server.ls.Put(
						storage.Type_BID,
						ord.Id,
						[]uint32{},
						false,
						"",
						"",
						bidUpdate,
						nil,
					)
				case *pqueue.Ask:
					askUpdate := &mcproto.AskStatus{
						Ask: &mcproto.Ask{
							Id:        ord.Id,
							ArtworkId: ord.ArtworkId,
							AskerId:   ord.AskerId,
							Quantity:  ord.Quantity(),
							Price:     ord.Price,
						},
						QuantityFilled: ord.QuantityFilled(),
						Status:         status,
					}
					server.ls.Put(
						storage.Type_ASK,
						ord.Id,
						[]uint32{},
						false,
						"",
						"",
						nil,
						askUpdate,
					)
				}

			}
		}
	}(server)

	return server
}

var (
	port               = flag.Int("port", 8082, "Server port")
	storageServicePort = flag.Int("storage-port", 8083, "Server port")
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
