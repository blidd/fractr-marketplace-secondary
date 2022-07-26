package main

import (
	"context"
	"fractr-marketplace-secondary/pqueue"
	"time"

	mcproto "github.com/blidd/fractr-proto/marketplace_common"
	msproto "github.com/blidd/fractr-proto/marketplace_secondary"
)

func (server *Server) PlaceBid(
	ctx context.Context,
	req *msproto.PlaceBidRequest,
) (*msproto.PlaceBidResponse, error) {

	bid := &pqueue.Bid{
		Id:             req.Bid.Id,
		BidderId:       req.Bid.BidderId,
		ArtworkId:      req.Bid.ArtworkId,
		Quantity:       req.Bid.Quantity,
		Price:          req.Bid.Price,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}

	pendingOrders := server.match.FillBidOrder(bid)

	ordersResp := make([]*mcproto.Order, 0)
	var quantityFilled uint32 = 0
	for _, order := range pendingOrders {
		ordersResp = append(ordersResp, &mcproto.Order{
			BidId:          order.BidId,
			AskId:          order.AskId,
			ArtworkId:      order.ArtworkId,
			Price:          order.Price,
			QuantityFilled: order.QuantityFilled,
			Status:         order.Status,
		})
		quantityFilled += order.QuantityFilled
	}

	return &msproto.PlaceBidResponse{
		Orders:         ordersResp,
		QuantityFilled: quantityFilled,
	}, nil
}

func (server *Server) PlaceAsk(
	ctx context.Context,
	req *msproto.PlaceAskRequest,
) (*msproto.PlaceAskResponse, error) {

	ask := &pqueue.Ask{
		Id:             req.Ask.Id,
		AskerId:        req.Ask.AskerId,
		ArtworkId:      req.Ask.ArtworkId,
		Quantity:       req.Ask.Quantity,
		Price:          req.Ask.Price,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}

	pendingOrders := server.match.FillAskOrder(ask)

	ordersResp := make([]*mcproto.Order, 0)
	var quantityFilled uint32 = 0
	for _, order := range pendingOrders {
		ordersResp = append(ordersResp, &mcproto.Order{
			BidId:          order.BidId,
			AskId:          order.AskId,
			ArtworkId:      order.ArtworkId,
			Price:          order.Price,
			QuantityFilled: order.QuantityFilled,
			Status:         order.Status,
		})
		quantityFilled += order.QuantityFilled
	}

	return &msproto.PlaceAskResponse{
		Orders:         ordersResp,
		QuantityFilled: quantityFilled,
	}, nil

}
