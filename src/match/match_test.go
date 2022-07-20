package match

import (
	"container/heap"
	"fractr-marketplace-secondary/pqueue"
	"sync"
	"testing"
	"time"
)

func SetupServerOneArtwork(artworkId string) *Server {
	server := Server{
		bids:   make(map[string]*BidPriorityQueueMutex),
		asks:   make(map[string]*AskPriorityQueueMutex),
		orders: make(chan FillOrder),
	}

	bidPQ := make(pqueue.BidPriorityQueue, 0)
	heap.Init(&bidPQ)
	askPQ := make(pqueue.AskPriorityQueue, 0)
	heap.Init(&askPQ)

	server.bids[artworkId] = &BidPriorityQueueMutex{pqueue: &bidPQ, mu: &sync.Mutex{}}
	server.asks[artworkId] = &AskPriorityQueueMutex{pqueue: &askPQ, mu: &sync.Mutex{}}

	return &server
}

func TestFillBidOrderHigherAndLowerAsks(t *testing.T) {
	artworkId := randString(10)

	server := SetupServerOneArtwork(artworkId)

	bid := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}

	ask0 := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       50,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	ask1 := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          12,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	ask2 := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       20,
		Price:          9,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	heap.Push(server.asks[artworkId].pqueue, &ask0)
	heap.Push(server.asks[artworkId].pqueue, &ask1)
	heap.Push(server.asks[artworkId].pqueue, &ask2)
	go server.fillBidOrder(&bid)

	numOrders := 0
	for {
		select {
		case order := <-server.orders:
			numOrders += 1
			if bid.Price < order.price {
				t.Errorf("Cannot sell bid at %d at %d", bid.Price, order.price)
			}
			t.Logf("Bid sold for %d at asking price %d", bid.Price, order.price)
		case <-time.After(time.Second * 1):
			if numOrders != 2 {
				t.Errorf("Wrong number of orders")
			}
			return
		}
	}
}

func TestFillBidOrderPartiallyUnfilled(t *testing.T) {
	artworkId := randString(10)

	server := SetupServerOneArtwork(artworkId)

	bid := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}

	ask0 := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       50,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	ask1 := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       20,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	ask2 := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          11,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	heap.Push(server.asks[artworkId].pqueue, &ask0)
	heap.Push(server.asks[artworkId].pqueue, &ask1)
	heap.Push(server.asks[artworkId].pqueue, &ask2)
	go server.fillBidOrder(&bid)

	numOrders := 0
	for {
		select {
		case order := <-server.orders:
			numOrders += 1
			t.Logf("Bid quantity %d sold for %d at asking price %d", order.quantityFilled, bid.Price, order.price)
		case <-time.After(time.Second * 2):
			if numOrders != 2 {
				t.Fatalf("Expected 2 orders, received %d", numOrders)
			}
			if server.bids[artworkId].pqueue.Len() == 0 {
				t.Fatalf("Failed to insert bid into queue")
			}
			bid := heap.Pop(server.bids[artworkId].pqueue).(*pqueue.Bid)
			if bid.QuantityRemaining() != 30 {
				t.Errorf("Bid quantity remaining incorrect: found %d, should have %d", bid.QuantityRemaining(), 30)
			}
			return
		}
	}
}
