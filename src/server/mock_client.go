package main

import (
	"context"

	msproto "github.com/blidd/fractr-proto/marketplace_secondary"
)

type MockClient struct {
	inMemServer *Server
}

func NewMockClient() *MockClient {
	return &MockClient{
		inMemServer: New(),
	}
}

func (client *MockClient) PlaceBid(
	ctx context.Context,
	req *msproto.PlaceBidRequest,
) (*msproto.PlaceBidResponse, error) {
	return client.inMemServer.PlaceBid(ctx, req)
}

func (client *MockClient) PlaceAsk(
	ctx context.Context,
	req *msproto.PlaceAskRequest,
) (*msproto.PlaceAskResponse, error) {
	return client.inMemServer.PlaceAsk(ctx, req)
}
