package main

import (
	"container/heap"
	"fmt"
	"time"
)

type Bid struct {
	id        string
	bidderId  string
	artworkId string
	quantity  int32
	price     int32
	status    int32
	placedAt  time.Time

	quantityFilled int32

	index int // for heap interface
}

func (bid *Bid) QuantityRemaining() int32 {
	return bid.quantity - bid.quantityFilled
}

type BidPriorityQueue []*Bid

func (bpq BidPriorityQueue) Len() int { return len(bpq) }

func (bpq BidPriorityQueue) Less(i, j int) bool {
	if bpq[i].price > bpq[j].price {
		return true
	} else if bpq[i].price < bpq[j].price {
		return false
	} else { // if prices are equal, prioritize earlier order
		return bpq[i].placedAt.Before(bpq[j].placedAt)
	}
}

func (bpq BidPriorityQueue) Swap(i, j int) {
	bpq[i], bpq[j] = bpq[j], bpq[i]
	bpq[i].index = i
	bpq[j].index = j
}

func (bpq *BidPriorityQueue) Push(bid interface{}) {
	n := len(*bpq)
	item := bid.(*Bid)
	item.index = n
	*bpq = append(*bpq, item)
}

func (bpq *BidPriorityQueue) Pop() interface{} {
	old := *bpq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*bpq = old[0 : n-1]
	return item
}

type Ask struct {
	id        string
	askerId   string
	artworkId string
	quantity  int32
	price     int32
	status    int32
	placedAt  time.Time

	quantityFilled int32

	index int
}

func (ask *Ask) QuantityRemaining() int32 {
	return ask.quantity - ask.quantityFilled
}

type AskPriorityQueue []*Ask

func (apq AskPriorityQueue) Len() int { return len(apq) }

func (apq AskPriorityQueue) Less(i, j int) bool {
	if apq[i].price < apq[j].price {
		return true
	} else if apq[i].price > apq[j].price {
		return false
	} else { // if prices are equal, prioritize earlier order
		return apq[i].placedAt.Before(apq[j].placedAt)
	}
}

func (apq AskPriorityQueue) Swap(i, j int) {
	apq[i], apq[j] = apq[j], apq[i]
	apq[i].index = i
	apq[j].index = j
}

func (apq *AskPriorityQueue) Push(ask interface{}) {
	n := len(*apq)
	item := ask.(*Ask)
	item.index = n
	*apq = append(*apq, item)
}

func (apq *AskPriorityQueue) Pop() interface{} {
	old := *apq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*apq = old[0 : n-1]
	return item
}

func (apq AskPriorityQueue) Peek() *Ask {
	if apq.Len() == 0 {
		return &Ask{}
	}
	return apq[0]
}

func testAsk() {
	time0, _ := time.Parse(time.RFC822, "01 Jan 14 10:00 UTC")
	time1, _ := time.Parse(time.RFC822, "01 Jan 14 10:01 UTC")
	time2, _ := time.Parse(time.RFC822, "01 Jan 14 10:02 UTC")
	time3, _ := time.Parse(time.RFC822, "01 Jan 14 10:03 UTC")
	time4, _ := time.Parse(time.RFC822, "01 Jan 14 10:04 UTC")
	time5, _ := time.Parse(time.RFC822, "01 Jan 15 10:00 UTC")
	asks := map[string]*Ask{
		"0": {
			quantity: 20,
			price:    10,
			placedAt: time0,
		},
		"1": {
			quantity: 30,
			price:    11,
			placedAt: time1,
		},
		"2": {
			quantity: 30,
			price:    8,
			placedAt: time2,
		},
		"3": {
			quantity: 30,
			price:    7,
			placedAt: time3,
		},
		"4": {
			quantity: 30,
			price:    12,
			placedAt: time4,
		},
		"5": {
			quantity: 30,
			price:    10,
			placedAt: time5,
		},
	}

	apq := make(AskPriorityQueue, len(asks))
	i := 0
	for _, ask := range asks {
		apq[i] = ask
		i++
	}
	heap.Init(&apq)

	time6, _ := time.Parse(time.RFC822, "01 Jan 15 11:00 UTC")
	heap.Push(&apq, &Ask{
		quantity: 30,
		price:    9,
		placedAt: time6,
	})

	for apq.Len() > 0 {
		ask := heap.Pop(&apq).(*Ask)
		// fmt.Printf("next min: %+v\n", apq[0])
		fmt.Printf("price: %v qty: %v time: %v\n", ask.price, ask.quantity, ask.placedAt)
	}

}

func testBid() {
	time0, _ := time.Parse(time.RFC822, "01 Jan 14 10:00 UTC")
	time1, _ := time.Parse(time.RFC822, "01 Jan 14 10:01 UTC")
	time2, _ := time.Parse(time.RFC822, "01 Jan 14 10:02 UTC")
	time3, _ := time.Parse(time.RFC822, "01 Jan 14 10:03 UTC")
	time4, _ := time.Parse(time.RFC822, "01 Jan 14 10:04 UTC")
	time5, _ := time.Parse(time.RFC822, "01 Jan 15 10:00 UTC")
	bids := map[string]*Bid{
		"0": {
			quantity: 20,
			price:    10,
			placedAt: time0,
		},
		"1": {
			quantity: 30,
			price:    11,
			placedAt: time1,
		},
		"2": {
			quantity: 30,
			price:    8,
			placedAt: time2,
		},
		"3": {
			quantity: 30,
			price:    7,
			placedAt: time3,
		},
		"4": {
			quantity: 30,
			price:    12,
			placedAt: time4,
		},
		"5": {
			quantity: 30,
			price:    10,
			placedAt: time5,
		},
	}

	bpq := make(BidPriorityQueue, len(bids))
	i := 0
	for _, bid := range bids {
		bpq[i] = bid
		i++
	}
	heap.Init(&bpq)

	time6, _ := time.Parse(time.RFC822, "01 Jan 15 11:00 UTC")
	heap.Push(&bpq, &Bid{
		quantity: 30,
		price:    9,
		placedAt: time6,
	})

	for bpq.Len() > 0 {
		bid := heap.Pop(&bpq).(*Bid)
		fmt.Printf("price: %v qty: %v time: %v\n", bid.price, bid.quantity, bid.placedAt)
	}
}
