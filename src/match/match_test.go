package match

import (
	"container/heap"
	"fractr-marketplace-secondary/pqueue"
	"sync"
	"testing"
	"time"
)

func SetupServerOneArtwork(artworkId uint32) *OrderMatchingEngine {
	server := OrderMatchingEngine{
		bids:   make(map[uint32]*BidPriorityQueueMutex),
		asks:   make(map[uint32]*AskPriorityQueueMutex),
		mu:     make(map[uint32]*sync.Mutex),
		orders: make(chan FillOrder),
	}

	bidPQ := make(pqueue.BidPriorityQueue, 0)
	heap.Init(&bidPQ)
	askPQ := make(pqueue.AskPriorityQueue, 0)
	heap.Init(&askPQ)

	server.mu[artworkId] = &sync.Mutex{}

	server.bids[artworkId] = &BidPriorityQueueMutex{pqueue: &bidPQ, mu: &sync.Mutex{}}
	server.asks[artworkId] = &AskPriorityQueueMutex{pqueue: &askPQ, mu: &sync.Mutex{}}

	return &server
}

func TestFillBidOrderHigherAndLowerAsks(t *testing.T) {
	var artworkId uint32 = 0

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
		case <-time.After(time.Second * 1):
			server.bids[artworkId].mu.Lock()
			defer server.bids[artworkId].mu.Unlock()
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

func TestFillAskOrderHigherAndLowerBids(t *testing.T) {
	artworkId := randString(10)

	server := SetupServerOneArtwork(artworkId)

	ask := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}

	bid0 := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       50,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	bid1 := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       30,
		Price:          12,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	bid2 := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       20,
		Price:          9,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	heap.Push(server.bids[artworkId].pqueue, &bid0)
	heap.Push(server.bids[artworkId].pqueue, &bid1)
	heap.Push(server.bids[artworkId].pqueue, &bid2)
	go server.fillAskOrder(&ask)

	numOrders := 0
	for {
		select {
		case order := <-server.orders:
			numOrders += 1
			if order.price < ask.Price {
				t.Errorf("Cannot sell ask at %d for %d", ask.Price, order.price)
			}
			t.Logf("Ask sold for %d with asking price %d", order.price, ask.Price)
		case <-time.After(time.Second * 1):
			if numOrders != 2 {
				t.Errorf("Wrong number of orders")
			}
			return
		}
	}
}

func TestFillAskOrderPartiallyUnfilled(t *testing.T) {
	artworkId := randString(10)

	server := SetupServerOneArtwork(artworkId)

	ask := pqueue.Ask{
		Id:             randString(10),
		AskerId:        randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}

	bid0 := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       50,
		Price:          9,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	bid1 := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       20,
		Price:          12,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	bid2 := pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       50,
		Price:          11,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	heap.Push(server.bids[artworkId].pqueue, &bid0)
	heap.Push(server.bids[artworkId].pqueue, &bid1)
	heap.Push(server.bids[artworkId].pqueue, &bid2)
	go server.fillAskOrder(&ask)

	numOrders := 0
	for {
		select {
		case order := <-server.orders:
			numOrders += 1
			t.Logf("Ask quantity %d sold for %d with original asking price %d", order.quantityFilled, order.price, ask.Price)
		case <-time.After(time.Second * 1):
			server.asks[artworkId].mu.Lock()
			defer server.asks[artworkId].mu.Unlock()
			if numOrders != 2 {
				t.Fatalf("Expected 2 orders, received %d", numOrders)
			}
			if server.asks[artworkId].pqueue.Len() == 0 {
				t.Fatalf("Failed to insert ask into queue")
			}
			ask := heap.Pop(server.asks[artworkId].pqueue).(*pqueue.Ask)
			if ask.QuantityRemaining() != 30 {
				t.Errorf("Ask quantity remaining incorrect: found %d, should have %d", ask.QuantityRemaining(), 30)
			}
			return
		}
	}
}

func TestFillBidAndAskOrdersConcurrent(t *testing.T) {
	artworkId := randString(10)

	server := SetupServerOneArtwork(artworkId)

	for i := 0; i < 3; i++ {
		// randPrice := rand.Intn(10) + 10 // random price between 10 and 20
		// randQuantity := rand.Intn

		ask := pqueue.Ask{
			Id:             randString(10),
			AskerId:        randString(10),
			ArtworkId:      artworkId,
			Quantity:       100,
			Price:          int32(10 + i),
			PlacedAt:       time.Now(),
			QuantityFilled: 0,
		}
		// t.Logf("Ask: %+v", ask)
		go server.fillAskOrder(&ask)

		bid := pqueue.Bid{
			Id:             randString(10),
			BidderId:       randString(10),
			ArtworkId:      artworkId,
			Quantity:       100,
			Price:          int32(10 + i),
			PlacedAt:       time.Now(),
			QuantityFilled: 0,
		}
		// t.Logf("Bid: %+v", bid)
		go server.fillBidOrder(&bid)

	}

	numOrders := 0
	for {
		select {
		case order := <-server.orders:
			numOrders += 1
			// if bid.Price < order.price {
			// 	t.Errorf("Cannot sell bid at %d at %d", bid.Price, order.price)
			// }
			t.Logf("Order: %+v", order)
		case <-time.After(time.Second * 1):
			if numOrders < 2 || numOrders > 3 {
				t.Fatalf("Wrong number of orders: expected 2-3, got %d", numOrders)
			}
			return
		}
	}
}

func TestFillBidAndAskOrderTimeOrdering(t *testing.T) {

	artworkId := randString(10)

	server := SetupServerOneArtwork(artworkId)

	for i := 0; i < 3; i++ {
		ask := pqueue.Ask{
			Id:             randString(10),
			AskerId:        randString(10),
			ArtworkId:      artworkId,
			Quantity:       int32(100 + i),
			Price:          10,
			PlacedAt:       time.Now(),
			QuantityFilled: 0,
		}
		server.addAsk(&ask)
		time.Sleep(1 * time.Second)
	}

	bid := &pqueue.Bid{
		Id:             randString(10),
		BidderId:       randString(10),
		ArtworkId:      artworkId,
		Quantity:       100,
		Price:          10,
		PlacedAt:       time.Now(),
		QuantityFilled: 0,
	}
	go server.fillBidOrder(bid)

	for {
		select {
		case order := <-server.orders:
			t.Logf("Bid sold for %d at asking price %d", bid.Price, order.price)
			if bid.Price < order.price {
				t.Fatalf("Cannot sell bid at %d at %d", bid.Price, order.price)
			}
			if order.quantityFilled != 100 {
				t.Fatalf("Filled a later order")
			}
			return
		case <-time.After(time.Second * 1):
			t.Fatalf("Order unfilled")
		}
	}

}
