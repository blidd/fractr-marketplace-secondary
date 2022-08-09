package match

import (
	"container/heap"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"sync"

	"fractr-marketplace-secondary/pqueue"
)

// possible statuses
const (
	ORDER_PENDING = iota
	ORDER_COMPLETE
	ORDER_REJECTED
)

type BidAsk interface {
	Quantity() uint32
	QuantityFilled() uint32
}

type OrderMatchingEngine struct {
	bids   map[uint32]*BidPriorityQueueMutex // key: artworkId
	asks   map[uint32]*AskPriorityQueueMutex // key: artworkId
	mu     map[uint32]*sync.Mutex
	orders chan FillOrder
	jobs   chan BidAsk
}

type BidPriorityQueueMutex struct {
	pqueue *pqueue.BidPriorityQueue
	mu     *sync.Mutex
}

type AskPriorityQueueMutex struct {
	pqueue *pqueue.AskPriorityQueue
	mu     *sync.Mutex
}

type FillOrder struct {
	BidId          uint32
	AskId          uint32
	ArtworkId      uint32
	Price          uint32
	QuantityFilled uint32
	Status         uint32
}

func New() *OrderMatchingEngine {
	return &OrderMatchingEngine{
		bids:   make(map[uint32]*BidPriorityQueueMutex),
		asks:   make(map[uint32]*AskPriorityQueueMutex),
		mu:     make(map[uint32]*sync.Mutex),
		orders: make(chan FillOrder),
	}
}

func (ome *OrderMatchingEngine) AddArtworkIfNotExists(artworkId uint32) {
	if ome.mu[artworkId] == nil {
		bidPQ := make(pqueue.BidPriorityQueue, 0)
		heap.Init(&bidPQ)
		askPQ := make(pqueue.AskPriorityQueue, 0)
		heap.Init(&askPQ)

		ome.bids[artworkId] = &BidPriorityQueueMutex{pqueue: &bidPQ, mu: &sync.Mutex{}}
		ome.asks[artworkId] = &AskPriorityQueueMutex{pqueue: &askPQ, mu: &sync.Mutex{}}
		ome.mu[artworkId] = &sync.Mutex{}
	}
}

func (ome *OrderMatchingEngine) Orders() chan FillOrder {
	return ome.orders
}

func (ome *OrderMatchingEngine) Jobs() chan BidAsk {
	return ome.jobs
}

func (ome *OrderMatchingEngine) AddAsk(ask *pqueue.Ask) {

	if ome.asks[ask.ArtworkId] == nil {
		askPQ := make(pqueue.AskPriorityQueue, 0)
		heap.Init(&askPQ)
		ome.asks[ask.ArtworkId] = &AskPriorityQueueMutex{pqueue: &askPQ, mu: &sync.Mutex{}}
	}
	heap.Push(ome.asks[ask.ArtworkId].pqueue, ask)
}

func (ome *OrderMatchingEngine) AddBid(bid *pqueue.Bid) {
	// check if queue exists
	if ome.bids[bid.ArtworkId] == nil {
		bidPQ := make(pqueue.BidPriorityQueue, 0)
		heap.Init(&bidPQ)
		ome.bids[bid.ArtworkId] = &BidPriorityQueueMutex{pqueue: &bidPQ, mu: &sync.Mutex{}}
	}
	heap.Push(ome.bids[bid.ArtworkId].pqueue, bid)
}

func (ome *OrderMatchingEngine) FillAskOrder(ask *pqueue.Ask) *pqueue.Ask {
	ome.AddArtworkIfNotExists(ask.ArtworkId)

	ome.mu[ask.ArtworkId].Lock()
	defer ome.mu[ask.ArtworkId].Unlock()

	bid := ome.bids[ask.ArtworkId].pqueue.Peek()
	for ome.bids[ask.ArtworkId].pqueue.Len() > 0 && ask.Price <= bid.Price {

		quantityToFill := math.Min(float64(ask.QuantityRemaining()), float64(bid.QuantityRemaining()))
		ask.FillQuantity(uint32(quantityToFill))
		bid.FillQuantity(uint32(quantityToFill))

		fmt.Println("sending job...")
		ome.jobs <- bid

		order := FillOrder{
			BidId:          bid.Id,
			AskId:          ask.Id,
			ArtworkId:      ask.ArtworkId,
			Price:          bid.Price,
			QuantityFilled: uint32(quantityToFill),
			Status:         ORDER_PENDING,
		}

		ome.orders <- order

		if bid.QuantityRemaining() == 0 {
			heap.Pop(ome.bids[ask.ArtworkId].pqueue)
		}
		bid = ome.bids[ask.ArtworkId].pqueue.Peek()

		if ask.QuantityRemaining() == 0 {
			break
		}
	}

	if ask.QuantityRemaining() > 0 {
		ome.AddAsk(ask)
	}

	ome.jobs <- ask

	return ask
}

func (ome *OrderMatchingEngine) FillBidOrder(bid *pqueue.Bid) *pqueue.Bid {
	ome.AddArtworkIfNotExists(bid.ArtworkId)

	ome.mu[bid.ArtworkId].Lock()
	defer ome.mu[bid.ArtworkId].Unlock()

	// TODO: What if the ask queue is empty?
	ask := ome.asks[bid.ArtworkId].pqueue.Peek()
	for ome.asks[bid.ArtworkId].pqueue.Len() > 0 && ask.Price <= bid.Price {

		quantityToFill := math.Min(float64(ask.QuantityRemaining()), float64(bid.QuantityRemaining()))
		ask.FillQuantity(uint32(quantityToFill))
		bid.FillQuantity(uint32(quantityToFill))
		// update storage
		fmt.Println("sending job...")
		ome.jobs <- ask

		// create order transaction
		order := FillOrder{
			BidId:          bid.Id,
			AskId:          ask.Id,
			ArtworkId:      bid.ArtworkId,
			Price:          ask.Price,
			QuantityFilled: uint32(quantityToFill),
			Status:         ORDER_PENDING,
		}

		ome.orders <- order

		// remove ask from queue if ask is complete
		if ask.QuantityRemaining() == 0 {
			heap.Pop(ome.asks[bid.ArtworkId].pqueue)
		}
		ask = ome.asks[bid.ArtworkId].pqueue.Peek()

		// finish up if the bid is complete
		if bid.QuantityRemaining() == 0 {
			break
		}
	}

	// if the bid is not yet completely filled, insert into queue
	if bid.QuantityRemaining() > 0 {
		ome.bids[bid.ArtworkId].mu.Lock()
		defer ome.bids[bid.ArtworkId].mu.Unlock()
		ome.AddBid(bid)
	}

	ome.jobs <- bid

	return bid
}

func randString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}
