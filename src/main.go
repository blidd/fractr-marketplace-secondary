package main

import (
	"fmt"
	"fractr-marketplace-secondary/pqueue"
)

func main() {

	// test priority queues
	fmt.Println("Test Bid...")
	pqueue.TestBid()
	fmt.Println("Test Ask...")
	pqueue.TestAsk()
	// TODO: what if one of the queues are empty?

	// match.TestFillBidOrder()

}
