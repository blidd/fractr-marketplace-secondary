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
		jobs:   make(chan BidAsk),
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

	match := SetupServerOneArtwork(artworkId)

	bid := pqueue.NewBid(
		1000,
		3000,
		artworkId,
		100,
		10,
	)

	ask0 := pqueue.NewAsk(
		2000,
		4000,
		artworkId,
		50,
		10,
	)

	ask1 := pqueue.NewAsk(
		2001,
		4001,
		artworkId,
		100,
		12,
	)

	ask2 := pqueue.NewAsk(
		2002,
		4002,
		artworkId,
		20,
		9,
	)

	match.AddAsk(ask0)
	match.AddAsk(ask1)
	match.AddAsk(ask2)

	go match.FillBidOrder(bid)

	numOrders := 0
	numJobs := 0
	for {
		select {
		case order := <-match.Orders():
			numOrders += 1
			t.Logf("ORDER: %+v\n", order)
		case job := <-match.Jobs():
			numJobs += 1
			t.Logf("JOB: %+v\n", job)
		case <-time.After(time.Second * 1):
			if numOrders != 2 {
				t.Fatalf("Expected 3 orders, received %d", numOrders)
			}
			if numJobs != 3 {
				t.Fatalf("Expected 3 jobs, received %d", numJobs)
			}
			return
		}
	}
}

type ExpectedJob struct {
	id             uint32
	quantity       uint32
	price          uint32
	quantityFilled uint32
}

func TestFillBidOrderAskPartiallyUnfilled(t *testing.T) {
	artworkId := uint32(0)

	match := SetupServerOneArtwork(artworkId)

	bid := pqueue.NewBid(
		1000,
		3000,
		artworkId,
		100,
		10,
	)

	ask0 := pqueue.NewAsk(
		2000,
		4000,
		artworkId,
		50,
		8,
	)

	ask1 := pqueue.NewAsk(
		2001,
		4001,
		artworkId,
		20,
		9,
	)

	ask2 := pqueue.NewAsk(
		2002,
		4002,
		artworkId,
		100,
		10,
	)

	match.AddAsk(ask0)
	match.AddAsk(ask1)
	match.AddAsk(ask2)

	go match.FillBidOrder(bid)

	numOrders := 0

	// expectedJobs := []ExpectedJob{
	// 	{id: 2000, quantity: 50, price: 8, quantityFilled: 50},
	// 	{id: 2001, quantity: 20, price: 9, quantityFilled: 20},
	// 	{id: 2002, quantity: 100, price: 10, quantityFilled: 30},
	// }

	numJobs := 0
	for {
		select {
		case order := <-match.Orders():
			numOrders += 1
			t.Logf("ORDER: %+v\n", order)
		case job := <-match.Jobs():
			numJobs += 1
			t.Logf("JOB: %+v\n", job)
		case <-time.After(time.Second * 1):
			return
		}
	}
}

func TestFillAskOrderHigherAndLowerBids(t *testing.T) {
	artworkId := uint32(0)

	match := SetupServerOneArtwork(artworkId)

	ask := pqueue.NewAsk(
		1000,
		3000,
		artworkId,
		100,
		10,
	)

	bid0 := pqueue.NewBid(
		2000,
		4000,
		artworkId,
		50,
		10,
	)

	bid1 := pqueue.NewBid(
		2001,
		4001,
		artworkId,
		30,
		12,
	)

	bid2 := pqueue.NewBid(
		2002,
		4002,
		artworkId,
		20,
		9,
	)

	match.AddBid(bid0)
	match.AddBid(bid1)
	match.AddBid(bid2)

	go match.FillAskOrder(ask)

	numOrders := 0
	numJobs := 0
	for {
		select {
		case order := <-match.Orders():
			numOrders += 1
			t.Logf("ORDER: %+v\n", order)
		case job := <-match.Jobs():
			numJobs += 1
			t.Logf("JOB: %+v\n", job)
		case <-time.After(time.Second * 1):
			if numOrders != 2 {
				t.Fatalf("Expected 3 orders, received %d", numOrders)
			}
			if numJobs != 3 {
				t.Fatalf("Expected 3 jobs, received %d", numJobs)
			}
			return
		}
	}
}

// func TestFillAskOrderPartiallyUnfilled(t *testing.T) {
// 	artworkId := randString(10)

// 	server := SetupServerOneArtwork(artworkId)

// 	ask := pqueue.Ask{
// 		Id:             randString(10),
// 		AskerId:        randString(10),
// 		ArtworkId:      artworkId,
// 		Quantity:       100,
// 		Price:          10,
// 		PlacedAt:       time.Now(),
// 		QuantityFilled: 0,
// 	}

// 	bid0 := pqueue.Bid{
// 		Id:             randString(10),
// 		BidderId:       randString(10),
// 		ArtworkId:      artworkId,
// 		Quantity:       50,
// 		Price:          9,
// 		PlacedAt:       time.Now(),
// 		QuantityFilled: 0,
// 	}
// 	bid1 := pqueue.Bid{
// 		Id:             randString(10),
// 		BidderId:       randString(10),
// 		ArtworkId:      artworkId,
// 		Quantity:       20,
// 		Price:          12,
// 		PlacedAt:       time.Now(),
// 		QuantityFilled: 0,
// 	}
// 	bid2 := pqueue.Bid{
// 		Id:             randString(10),
// 		BidderId:       randString(10),
// 		ArtworkId:      artworkId,
// 		Quantity:       50,
// 		Price:          11,
// 		PlacedAt:       time.Now(),
// 		QuantityFilled: 0,
// 	}
// 	heap.Push(server.bids[artworkId].pqueue, &bid0)
// 	heap.Push(server.bids[artworkId].pqueue, &bid1)
// 	heap.Push(server.bids[artworkId].pqueue, &bid2)
// 	go server.fillAskOrder(&ask)

// 	numOrders := 0
// 	for {
// 		select {
// 		case order := <-server.orders:
// 			numOrders += 1
// 			t.Logf("Ask quantity %d sold for %d with original asking price %d", order.quantityFilled, order.price, ask.Price)
// 		case <-time.After(time.Second * 1):
// 			server.asks[artworkId].mu.Lock()
// 			defer server.asks[artworkId].mu.Unlock()
// 			if numOrders != 2 {
// 				t.Fatalf("Expected 2 orders, received %d", numOrders)
// 			}
// 			if server.asks[artworkId].pqueue.Len() == 0 {
// 				t.Fatalf("Failed to insert ask into queue")
// 			}
// 			ask := heap.Pop(server.asks[artworkId].pqueue).(*pqueue.Ask)
// 			if ask.QuantityRemaining() != 30 {
// 				t.Errorf("Ask quantity remaining incorrect: found %d, should have %d", ask.QuantityRemaining(), 30)
// 			}
// 			return
// 		}
// 	}
// }

// func TestFillBidAndAskOrdersConcurrent(t *testing.T) {
// 	artworkId := randString(10)

// 	server := SetupServerOneArtwork(artworkId)

// 	for i := 0; i < 3; i++ {
// 		// randPrice := rand.Intn(10) + 10 // random price between 10 and 20
// 		// randQuantity := rand.Intn

// 		ask := pqueue.Ask{
// 			Id:             randString(10),
// 			AskerId:        randString(10),
// 			ArtworkId:      artworkId,
// 			Quantity:       100,
// 			Price:          int32(10 + i),
// 			PlacedAt:       time.Now(),
// 			QuantityFilled: 0,
// 		}
// 		// t.Logf("Ask: %+v", ask)
// 		go server.fillAskOrder(&ask)

// 		bid := pqueue.Bid{
// 			Id:             randString(10),
// 			BidderId:       randString(10),
// 			ArtworkId:      artworkId,
// 			Quantity:       100,
// 			Price:          int32(10 + i),
// 			PlacedAt:       time.Now(),
// 			QuantityFilled: 0,
// 		}
// 		// t.Logf("Bid: %+v", bid)
// 		go server.fillBidOrder(&bid)

// 	}

// 	numOrders := 0
// 	for {
// 		select {
// 		case order := <-server.orders:
// 			numOrders += 1
// 			// if bid.Price < order.price {
// 			// 	t.Errorf("Cannot sell bid at %d at %d", bid.Price, order.price)
// 			// }
// 			t.Logf("Order: %+v", order)
// 		case <-time.After(time.Second * 1):
// 			if numOrders < 2 || numOrders > 3 {
// 				t.Fatalf("Wrong number of orders: expected 2-3, got %d", numOrders)
// 			}
// 			return
// 		}
// 	}
// }

// func TestFillBidAndAskOrderTimeOrdering(t *testing.T) {

// 	artworkId := randString(10)

// 	server := SetupServerOneArtwork(artworkId)

// 	for i := 0; i < 3; i++ {
// 		ask := pqueue.Ask{
// 			Id:             randString(10),
// 			AskerId:        randString(10),
// 			ArtworkId:      artworkId,
// 			Quantity:       int32(100 + i),
// 			Price:          10,
// 			PlacedAt:       time.Now(),
// 			QuantityFilled: 0,
// 		}
// 		server.addAsk(&ask)
// 		time.Sleep(1 * time.Second)
// 	}

// 	bid := &pqueue.Bid{
// 		Id:             randString(10),
// 		BidderId:       randString(10),
// 		ArtworkId:      artworkId,
// 		Quantity:       100,
// 		Price:          10,
// 		PlacedAt:       time.Now(),
// 		QuantityFilled: 0,
// 	}
// 	go server.fillBidOrder(bid)

// 	for {
// 		select {
// 		case order := <-server.orders:
// 			t.Logf("Bid sold for %d at asking price %d", bid.Price, order.price)
// 			if bid.Price < order.price {
// 				t.Fatalf("Cannot sell bid at %d at %d", bid.Price, order.price)
// 			}
// 			if order.quantityFilled != 100 {
// 				t.Fatalf("Filled a later order")
// 			}
// 			return
// 		case <-time.After(time.Second * 1):
// 			t.Fatalf("Order unfilled")
// 		}
// 	}

// }
