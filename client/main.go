package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	address     = "localhost:50051"
	defaultName = "world"
)

func main() {
	creds, err := credentials.NewClientTLSFromFile("../ssl/cert.pem", "")
	if err != nil {
		log.Fatalf("could not create credentials: %v", err)
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewSimpleDbClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	errChan := make(chan error)
	resultChan := make(chan pb.HostStatsReplyMsg)
	endChan := make(chan struct{}, 2)

	for i := 0; i < 2; i++ {
		go sendRequest(ctx, c, resultChan, errChan)
	}

	for {
		select {
		case val := <-resultChan:
			fmt.Printf("%+v\n", val)
			endChan <- struct{}{}
		case err := <-errChan:
			log.Fatalf("could not get hostname: %v", err)
		case <-endChan:
			return
		}
	}
	// r, err := c.GetHostInfo(ctx, &pb.Empty{})
	// if err != nil {
	// 	log.Fatalf("could not get hostname: %v", err)
	// }
	// log.Printf("Node hostname: %s", r.Hostname)
}

func sendRequest(ctx context.Context, c pb.SimpleDbClient, resultChan chan pb.HostStatsReplyMsg, errChan chan error) {
	r, err := c.GetHostInfoCaller(ctx, &pb.Empty{})
	if err != nil {
		errChan <- err
	}
	resultChan <- *r
}
