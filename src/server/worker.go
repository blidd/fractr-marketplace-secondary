// workers pull from the order channel to communicate orders details to
// the smart contract, database, and buyer/seller services

package main

import "fmt"

// run as async go routine
func (server *Server) Worker() {

	for {
		select {
		case order := <-server.match.Orders():
			fmt.Printf("%+v\n", order)
		}
	}

}
