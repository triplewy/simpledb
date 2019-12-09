package main

import (
	"log"
	"time"

	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	client pb.SimpleDbClient
}

func NewClient(addr string) *Client {
	c := connect(addr)
	return &Client{
		client: c,
	}
}

func connect(addr string) pb.SimpleDbClient {
	creds, err := credentials.NewClientTLSFromFile("../ssl/cert.pem", "")
	if err != nil {
		log.Fatalf("could not create credentials: %v", err)
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewSimpleDbClient(conn)
}
