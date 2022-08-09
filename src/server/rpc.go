package main

import (
	"context"
	"fractr-marketplace-secondary/pqueue"

	mcproto "github.com/blidd/fractr-proto/marketplace_common"
	msproto "github.com/blidd/fractr-proto/marketplace_secondary"
)

func (server *Server) PlaceBid(
	ctx context.Context,
	req *msproto.PlaceBidRequest,
) (*msproto.PlaceBidResponse, error) {

	bid := pqueue.NewBid(
		req.Bid.Id,
		req.Bid.BidderId,
		req.Bid.ArtworkId,
		req.Bid.Quantity,
		req.Bid.Price,
	)

	bidPlaced := server.match.FillBidOrder(bid)
	bidProto := &mcproto.Bid{
		Id:        bidPlaced.Id,
		ArtworkId: bidPlaced.ArtworkId,
		BidderId:  bidPlaced.BidderId,
		Quantity:  bidPlaced.Quantity(),
		Price:     bidPlaced.Price,
	}

	return &msproto.PlaceBidResponse{
		BidStatus: &mcproto.BidStatus{
			Bid:            bidProto,
			QuantityFilled: bidPlaced.QuantityFilled(),
			Status:         bidPlaced.Status(),
		},
	}, nil
}

func (server *Server) PlaceAsk(
	ctx context.Context,
	req *msproto.PlaceAskRequest,
) (*msproto.PlaceAskResponse, error) {

	ask := pqueue.NewAsk(
		req.Ask.Id,
		req.Ask.AskerId,
		req.Ask.ArtworkId,
		req.Ask.Quantity,
		req.Ask.Price,
	)

	askPlaced := server.match.FillAskOrder(ask)
	askProto := &mcproto.Ask{
		Id:        askPlaced.Id,
		ArtworkId: askPlaced.ArtworkId,
		AskerId:   askPlaced.AskerId,
		Quantity:  askPlaced.Quantity(),
		Price:     askPlaced.Price,
	}

	return &msproto.PlaceAskResponse{
		AskStatus: &mcproto.AskStatus{
			Ask:            askProto,
			QuantityFilled: askPlaced.QuantityFilled(),
			Status:         askPlaced.Status(),
		},
	}, nil
}
