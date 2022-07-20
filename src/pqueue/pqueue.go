package pqueue

import (
	"container/heap"
	"fmt"
	"time"
)

type Bid struct {
	Id        string
	BidderId  string
	ArtworkId string
	Quantity  int32
	Price     int32
	Status    int32
	PlacedAt  time.Time

	QuantityFilled int32

	index int // for heap interface
}

func (bid *Bid) QuantityRemaining() int32 {
	return bid.Quantity - bid.QuantityFilled
}

type BidPriorityQueue []*Bid

func (bpq BidPriorityQueue) Len() int { return len(bpq) }

func (bpq BidPriorityQueue) Less(i, j int) bool {
	if bpq[i].Price > bpq[j].Price {
		return true
	} else if bpq[i].Price < bpq[j].Price {
		return false
	} else { // if prices are equal, prioritize earlier order
		return bpq[i].PlacedAt.Before(bpq[j].PlacedAt)
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

func (bpq BidPriorityQueue) Peek() *Bid {
	if bpq.Len() == 0 {
		return &Bid{}
	}
	return bpq[0]
}

type Ask struct {
	Id        string
	AskerId   string
	ArtworkId string
	Quantity  int32
	Price     int32
	Status    int32
	PlacedAt  time.Time

	QuantityFilled int32

	index int
}

func (ask *Ask) QuantityRemaining() int32 {
	return ask.Quantity - ask.QuantityFilled
}

type AskPriorityQueue []*Ask

func (apq AskPriorityQueue) Len() int { return len(apq) }

func (apq AskPriorityQueue) Less(i, j int) bool {
	if apq[i].Price < apq[j].Price {
		return true
	} else if apq[i].Price > apq[j].Price {
		return false
	} else { // if prices are equal, prioritize earlier order
		return apq[i].PlacedAt.Before(apq[j].PlacedAt)
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

func TestAsk() {
	time0, _ := time.Parse(time.RFC822, "01 Jan 14 10:00 UTC")
	time1, _ := time.Parse(time.RFC822, "01 Jan 14 10:01 UTC")
	time2, _ := time.Parse(time.RFC822, "01 Jan 14 10:02 UTC")
	time3, _ := time.Parse(time.RFC822, "01 Jan 14 10:03 UTC")
	time4, _ := time.Parse(time.RFC822, "01 Jan 14 10:04 UTC")
	time5, _ := time.Parse(time.RFC822, "01 Jan 15 10:00 UTC")
	asks := map[string]*Ask{
		"0": {
			Quantity: 20,
			Price:    10,
			PlacedAt: time0,
		},
		"1": {
			Quantity: 30,
			Price:    11,
			PlacedAt: time1,
		},
		"2": {
			Quantity: 30,
			Price:    8,
			PlacedAt: time2,
		},
		"3": {
			Quantity: 30,
			Price:    7,
			PlacedAt: time3,
		},
		"4": {
			Quantity: 30,
			Price:    12,
			PlacedAt: time4,
		},
		"5": {
			Quantity: 30,
			Price:    10,
			PlacedAt: time5,
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
		Quantity: 30,
		Price:    9,
		PlacedAt: time6,
	})

	for apq.Len() > 0 {
		ask := heap.Pop(&apq).(*Ask)
		// fmt.Printf("next min: %+v\n", apq[0])
		fmt.Printf("price: %v qty: %v time: %v\n", ask.Price, ask.Quantity, ask.PlacedAt)
	}

}

func TestBid() {
	time0, _ := time.Parse(time.RFC822, "01 Jan 14 10:00 UTC")
	time1, _ := time.Parse(time.RFC822, "01 Jan 14 10:01 UTC")
	time2, _ := time.Parse(time.RFC822, "01 Jan 14 10:02 UTC")
	time3, _ := time.Parse(time.RFC822, "01 Jan 14 10:03 UTC")
	time4, _ := time.Parse(time.RFC822, "01 Jan 14 10:04 UTC")
	time5, _ := time.Parse(time.RFC822, "01 Jan 15 10:00 UTC")
	bids := map[string]*Bid{
		"0": {
			Quantity: 20,
			Price:    10,
			PlacedAt: time0,
		},
		"1": {
			Quantity: 30,
			Price:    11,
			PlacedAt: time1,
		},
		"2": {
			Quantity: 30,
			Price:    8,
			PlacedAt: time2,
		},
		"3": {
			Quantity: 30,
			Price:    7,
			PlacedAt: time3,
		},
		"4": {
			Quantity: 30,
			Price:    12,
			PlacedAt: time4,
		},
		"5": {
			Quantity: 30,
			Price:    10,
			PlacedAt: time5,
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
		Quantity: 30,
		Price:    9,
		PlacedAt: time6,
	})

	for bpq.Len() > 0 {
		bid := heap.Pop(&bpq).(*Bid)
		fmt.Printf("price: %v qty: %v time: %v\n", bid.Price, bid.Quantity, bid.PlacedAt)
	}
}
