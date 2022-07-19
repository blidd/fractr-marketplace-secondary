package match

import (
	"container/heap"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/blidd/fractr-marketplace-secondary/pqueue"
)

// possible statuses
const (
	ORDER_PENDING = iota
	ORDER_COMPLETE
	ORDER_REJECTED
)

type Server struct {
	bids   map[string]BidPriorityQueueMutex // key: artworkId
	asks   map[string]AskPriorityQueueMutex // key: artworkId
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
	price          int32
	quantityFilled int32
	status         int32
}

/*

BID 10 200 150 (50)

ASK 10 50 50 (0) x
ASK 10 100 100 (0) x

*/

func (server *Server) fillBidOrder(bid *pqueue.Bid) {
	server.asks[bid.artworkId].mu.Lock()
	defer server.asks[bid.artworkId].mu.Unlock()

	// TODO: What if the ask queue is empty?
	ask := server.asks[bid.artworkId].pqueue.Peek()
	for server.asks[bid.artworkId].pqueue.Len() > 0 && ask.price <= bid.price {

		// fmt.Printf("0 %+v\n", (*server.asks[bid.artworkId].pqueue)[0])
		// fmt.Printf("1 %+v\n", ask)

		quantityToFill := math.Min(float64(ask.QuantityRemaining()), float64(bid.QuantityRemaining()))
		ask.quantityFilled += int32(quantityToFill)
		bid.quantityFilled += int32(quantityToFill)

		// create order transaction
		server.orders <- FillOrder{
			bidId:          bid.id,
			askId:          ask.id,
			price:          ask.price,
			quantityFilled: int32(quantityToFill),
			status:         ORDER_PENDING,
		}

		// remove ask from queue if ask is complete
		if ask.QuantityRemaining() == 0 {
			heap.Pop(server.asks[bid.artworkId].pqueue)
		}
		ask = server.asks[bid.artworkId].pqueue.Peek()

		// finish up if the bid is complete
		if bid.QuantityRemaining() == 0 {
			break
		}
	}

	// if the bid is not yet completely filled, insert into queue
	// TODO: what if the queue doesn't exist yet?
	if bid.QuantityRemaining() > 0 {
		server.bids[bid.artworkId].mu.Lock()
		defer server.bids[bid.artworkId].mu.Unlock()
		heap.Push(server.bids[bid.artworkId].pqueue, bid)

	}
}

func (server *Server) worker() {

	for {
		order := <-server.orders
		fmt.Printf("price: %v quantity: %v\n", order.price, order.quantityFilled)
	}

}

func TestFillBidOrder() {

	server := Server{
		bids:   make(map[string]BidPriorityQueueMutex),
		asks:   make(map[string]AskPriorityQueueMutex),
		orders: make(chan FillOrder),
	}

	go server.worker()

	artworkId := randString(10)

	bidPQ := make(BidPriorityQueue, 0)
	heap.Init(&bidPQ)
	askPQ := make(AskPriorityQueue, 0)
	heap.Init(&askPQ)

	server.bids[artworkId] = BidPriorityQueueMutex{pqueue: &bidPQ, mu: &sync.Mutex{}}
	server.asks[artworkId] = AskPriorityQueueMutex{pqueue: &askPQ, mu: &sync.Mutex{}}

	bid := Bid{
		id:             randString(10),
		bidderId:       randString(10),
		artworkId:      artworkId,
		quantity:       200,
		price:          10,
		placedAt:       time.Now(),
		quantityFilled: 0,
	}
	ask0 := Ask{
		id:             randString(10),
		askerId:        randString(10),
		artworkId:      artworkId,
		quantity:       50,
		price:          10,
		placedAt:       time.Now(),
		quantityFilled: 0,
	}
	ask1 := Ask{
		id:             randString(10),
		askerId:        randString(10),
		artworkId:      artworkId,
		quantity:       100,
		price:          10,
		placedAt:       time.Now(),
		quantityFilled: 0,
	}

	heap.Push(server.asks[artworkId].pqueue, &ask0)
	heap.Push(server.asks[artworkId].pqueue, &ask1)
	server.fillBidOrder(&bid)
}

func randString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// func main() {

// 	// test priority queues
// 	fmt.Println("Test Bid...")
// 	testBid()
// 	fmt.Println("Test Ask...")
// 	testAsk()
// 	// TODO: what if one of the queues are empty?

// 	TestFillBidOrder()

// }

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
