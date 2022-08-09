package libstore

import (
	"context"
	"fmt"

	mcpb "github.com/blidd/fractr-proto/marketplace_common"
	pb "github.com/blidd/fractr-proto/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Libstore struct {
	StorageServiceAddr string
}

func NewLibstore(addr string) *Libstore {
	return &Libstore{
		StorageServiceAddr: addr,
	}
}

func (ls *Libstore) NewStorageServiceClient() (pb.StorageClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial(ls.StorageServiceAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to secondary market service: %v", err)
	}

	return pb.NewStorageClient(conn), nil
}

func (ls *Libstore) Put(
	docType pb.Type,
	id uint32,
	userIds []uint32,
	primaryMarket bool,
	artistName, artTitle string,
	bidStatus *mcpb.BidStatus,
	askStatus *mcpb.AskStatus,
) (*pb.PutResponse, error) {
	req := &pb.PutRequest{
		Type:          docType,
		Id:            &id,
		UserIds:       userIds,
		PrimaryMarket: &primaryMarket,
		ArtistName:    &artistName,
		ArtTitle:      &artTitle,
		BidStatus:     bidStatus,
		AskStatus:     askStatus,
	}
	client, err := ls.NewStorageServiceClient()
	if err != nil {
		return &pb.PutResponse{}, fmt.Errorf("error occurred while dialing storage service: %v", err)
	}

	return client.Put(context.Background(), req)

}
