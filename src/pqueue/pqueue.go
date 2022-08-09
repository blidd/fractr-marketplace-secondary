package pqueue

import (
	"container/heap"
	"fmt"
	"time"

	mcpb "github.com/blidd/fractr-proto/marketplace_common"
)

type Bid struct {
	Id        uint32
	BidderId  uint32
	ArtworkId uint32
	quantity  uint32
	Price     uint32
	PlacedAt  time.Time

	quantityFilled uint32

	index int // for heap interface
}

func NewBid(id, bidderId, artworkId, quantity, price uint32) *Bid {
	return &Bid{
		Id:             id,
		BidderId:       bidderId,
		ArtworkId:      artworkId,
		quantity:       quantity,
		Price:          price,
		PlacedAt:       time.Now(),
		quantityFilled: 0,
	}
}

func (bid *Bid) Quantity() uint32       { return bid.quantity }
func (bid *Bid) QuantityFilled() uint32 { return bid.quantityFilled }

func (bid *Bid) QuantityRemaining() uint32 {
	if bid.QuantityFilled() > bid.Quantity() {
		return 0
	}
	return bid.Quantity() - bid.QuantityFilled()
}

func (bid *Bid) FillQuantity(qty uint32) {
	bid.quantityFilled += qty
}

func (bid *Bid) Status() mcpb.Status {

	if bid.QuantityFilled() == bid.Quantity() {
		return mcpb.Status_COMPLETE
	} else if bid.QuantityFilled() > 0 {
		return mcpb.Status_PARTIALLY_FILLED
	} else {
		return mcpb.Status_NEW
	}
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
	Id        uint32
	AskerId   uint32
	ArtworkId uint32
	quantity  uint32
	Price     uint32
	PlacedAt  time.Time

	quantityFilled uint32

	index int
}

func NewAsk(id, askerId, artworkId, quantity, price uint32) *Ask {
	return &Ask{
		Id:             id,
		AskerId:        askerId,
		ArtworkId:      artworkId,
		quantity:       quantity,
		Price:          price,
		PlacedAt:       time.Now(),
		quantityFilled: 0,
	}
}

func (ask *Ask) Quantity() uint32       { return ask.quantity }
func (ask *Ask) QuantityFilled() uint32 { return ask.quantityFilled }

func (ask *Ask) QuantityRemaining() uint32 {
	if ask.QuantityFilled() > ask.Quantity() {
		return 0
	}
	return ask.Quantity() - ask.QuantityFilled()
}

func (ask *Ask) FillQuantity(qty uint32) {
	ask.quantityFilled += qty
}

func (ask *Ask) Status() mcpb.Status {

	if ask.QuantityFilled() == ask.Quantity() {
		return mcpb.Status_COMPLETE
	} else if ask.QuantityFilled() > 0 {
		return mcpb.Status_PARTIALLY_FILLED
	} else {
		return mcpb.Status_NEW
	}
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
			quantity: 20,
			Price:    10,
			PlacedAt: time0,
		},
		"1": {
			quantity: 30,
			Price:    11,
			PlacedAt: time1,
		},
		"2": {
			quantity: 30,
			Price:    8,
			PlacedAt: time2,
		},
		"3": {
			quantity: 30,
			Price:    7,
			PlacedAt: time3,
		},
		"4": {
			quantity: 30,
			Price:    12,
			PlacedAt: time4,
		},
		"5": {
			quantity: 30,
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
		quantity: 30,
		Price:    9,
		PlacedAt: time6,
	})

	for apq.Len() > 0 {
		ask := heap.Pop(&apq).(*Ask)
		// fmt.Printf("next min: %+v\n", apq[0])
		fmt.Printf("price: %v qty: %v time: %v\n", ask.Price, ask.Quantity(), ask.PlacedAt)
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
			quantity: 20,
			Price:    10,
			PlacedAt: time0,
		},
		"1": {
			quantity: 30,
			Price:    11,
			PlacedAt: time1,
		},
		"2": {
			quantity: 30,
			Price:    8,
			PlacedAt: time2,
		},
		"3": {
			quantity: 30,
			Price:    7,
			PlacedAt: time3,
		},
		"4": {
			quantity: 30,
			Price:    12,
			PlacedAt: time4,
		},
		"5": {
			quantity: 30,
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
		quantity: 30,
		Price:    9,
		PlacedAt: time6,
	})

	for bpq.Len() > 0 {
		bid := heap.Pop(&bpq).(*Bid)
		fmt.Printf("price: %v qty: %v time: %v\n", bid.Price, bid.Quantity(), bid.PlacedAt)
	}
}
