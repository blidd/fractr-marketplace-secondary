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

type Server struct {
	bids   map[string]*BidPriorityQueueMutex // key: artworkId
	asks   map[string]*AskPriorityQueueMutex // key: artworkId
	orders chan FillOrder
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
	bidId          string
	askId          string
	artworkId      string
	price          int32
	quantityFilled int32
	status         int32
}

/*

BID 10 200 150 (50)

ASK 10 50 50 (0) x
ASK 10 100 100 (0) x

*/

func (server *Server) addAsk(ask *pqueue.Ask) {

	if server.asks[ask.ArtworkId] == nil {
		askPQ := make(pqueue.AskPriorityQueue, 0)
		heap.Init(&askPQ)
		server.asks[ask.ArtworkId] = &AskPriorityQueueMutex{pqueue: &askPQ, mu: &sync.Mutex{}}
	}
	heap.Push(server.asks[ask.ArtworkId].pqueue, ask)
}

func (server *Server) addBid(bid *pqueue.Bid) {
	// check if queue exists
	if server.bids[bid.ArtworkId] == nil {
		bidPQ := make(pqueue.BidPriorityQueue, 0)
		heap.Init(&bidPQ)
		server.bids[bid.ArtworkId] = &BidPriorityQueueMutex{pqueue: &bidPQ, mu: &sync.Mutex{}}
	}
	heap.Push(server.bids[bid.ArtworkId].pqueue, bid)
}

func (server *Server) fillAskOrder(ask *pqueue.Ask) {
	server.bids[ask.ArtworkId].mu.Lock()
	defer server.bids[ask.ArtworkId].mu.Unlock()

	bid := server.bids[ask.ArtworkId].pqueue.Peek()
	for server.bids[ask.ArtworkId].pqueue.Len() > 0 && ask.Price <= bid.Price {

		quantityToFill := math.Min(float64(ask.QuantityRemaining()), float64(bid.QuantityRemaining()))
		ask.QuantityFilled += int32(quantityToFill)
		bid.QuantityFilled += int32(quantityToFill)

		server.orders <- FillOrder{
			bidId:          bid.Id,
			askId:          ask.Id,
			artworkId:      ask.ArtworkId,
			price:          ask.Price,
			quantityFilled: int32(quantityToFill),
			status:         ORDER_PENDING,
		}

		if bid.QuantityRemaining() == 0 {
			heap.Pop(server.bids[ask.ArtworkId].pqueue)
		}
		bid = server.bids[ask.ArtworkId].pqueue.Peek()

		if ask.QuantityRemaining() == 0 {
			break
		}
	}

	if ask.QuantityRemaining() > 0 {
		server.asks[ask.ArtworkId].mu.Lock()
		defer server.asks[ask.ArtworkId].mu.Unlock()
		server.addAsk(ask)
	}
}

func (server *Server) fillBidOrder(bid *pqueue.Bid) {
	server.asks[bid.ArtworkId].mu.Lock()
	defer server.asks[bid.ArtworkId].mu.Unlock()

	// TODO: What if the ask queue is empty?
	ask := server.asks[bid.ArtworkId].pqueue.Peek()
	for server.asks[bid.ArtworkId].pqueue.Len() > 0 && ask.Price <= bid.Price {

		quantityToFill := math.Min(float64(ask.QuantityRemaining()), float64(bid.QuantityRemaining()))
		ask.QuantityFilled += int32(quantityToFill)
		bid.QuantityFilled += int32(quantityToFill)

		// create order transaction
		server.orders <- FillOrder{
			bidId:          bid.Id,
			askId:          ask.Id,
			price:          ask.Price,
			quantityFilled: int32(quantityToFill),
			status:         ORDER_PENDING,
		}

		// remove ask from queue if ask is complete
		if ask.QuantityRemaining() == 0 {
			heap.Pop(server.asks[bid.ArtworkId].pqueue)
		}
		ask = server.asks[bid.ArtworkId].pqueue.Peek()

		// finish up if the bid is complete
		if bid.QuantityRemaining() == 0 {
			break
		}
	}

	// if the bid is not yet completely filled, insert into queue
	if bid.QuantityRemaining() > 0 {
		server.bids[bid.ArtworkId].mu.Lock()
		defer server.bids[bid.ArtworkId].mu.Unlock()
		server.addBid(bid)
	}
}

func (server *Server) worker() {
	for {
		order := <-server.orders
		fmt.Printf("price: %v quantity: %v\n", order.price, order.quantityFilled)
	}
}

func randString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// func New() *Server {
// 	return &Server{
// 		bidPool: newBidPool(),
// 		askPool: newAskPool(),
// 	}
// }

// func (server *Server) processBid(bid *Bid) {

// 	// insert into bid pool
// 	if _, ok := server.bidPool.queues[bid.artworkId]; ok {
// 		server.bidPool.queues[bid.artworkId] = append(server.bidPool.queues[bid.artworkId], bid)
// 	} else {
// 		server.bidPool.queues[bid.artworkId] = []*Bid{bid}
// 		server.bidPool.numArtworks++
// 	}
// 	server.bidPool.numBids++

// 	// TODO: check if bid matches any existing

// }

// func (server *Server) insertBid(bid *Bid) {

// }

// //
// func (server *Server) matchAndFillBids(bid *Bid) {

// 	// if asks, ok := server.askPool.queues[bid.artworkId]; ok {

// 	// }

// }

// func (server *Server) findBidMatches() {

// }

// func main() {
// 	test()
// }
