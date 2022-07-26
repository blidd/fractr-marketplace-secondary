package main

import (
	"context"
	"testing"

	mcproto "github.com/blidd/fractr-proto/marketplace_common"
	msproto "github.com/blidd/fractr-proto/marketplace_secondary"
)

func TestPlaceBidAndAsk(t *testing.T) {

	client := NewMockClient()

	reqBid := &msproto.PlaceBidRequest{
		Bid: &mcproto.Bid{
			Id:        0,
			ArtworkId: 1234,
			BidderId:  1234,
			Quantity:  100,
			Price:     10,
		},
	}
	respBid, err := client.PlaceBid(
		context.Background(),
		reqBid,
	)
	if err != nil {
		t.Fatalf("error returned when placing bid from client: %v\n", err)
	}
	t.Logf("PlaceBid() response orders: %v\n", respBid.Orders)
	t.Logf("PlaceBid() response quantity filled: %v\n", respBid.QuantityFilled)

	reqAsk := &msproto.PlaceAskRequest{
		Ask: &mcproto.Ask{
			Id:        0,
			ArtworkId: 1234,
			AskerId:   2345,
			Quantity:  100,
			Price:     10,
		},
	}
	respAsk, err := client.PlaceAsk(
		context.Background(),
		reqAsk,
	)
	if err != nil {
		t.Fatalf("error returned when placing ask from client: %v\n", err)
	}
	t.Logf("PlaceAsk() response orders: %v\n", respAsk.Orders)
	t.Logf("PlaceAsk() response quantity filled: %v\n", respAsk.QuantityFilled)

}
