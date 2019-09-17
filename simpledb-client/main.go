package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"gopkg.in/abiosoft/ishell.v1"
)

var addr string

func init() {
	flag.StringVar(&addr, "c", "", "Set join address, if any")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func printHelp(shell *ishell.Shell) {
	shell.Println("Commands:")
	shell.Println(" - help                    Prints this help message")
	shell.Println(" - connect                 Connect to a node")
	shell.Println(" - nodes                   Prints all remote nodes")
	shell.Println(" - stats                   Display node's stats")
	shell.Println(" - quit                    Shutdown node(s), then quit this CLI")
}

func main() {
	flag.Parse()

	c := connect(addr)

	shell := ishell.New()

	printHelp(shell)

	shell.Register("nodes", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		reply, err := c.GetNodesCaller(ctx, &pb.Empty{})
		if err != nil {
			return "", err
		}

		out, err := json.Marshal(reply)
		if err != nil {
			panic(err)
		}

		return string(out), nil
	})

	shell.Register("stats", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		reply, err := c.GetHostInfoCaller(ctx, &pb.Empty{})
		if err != nil {
			return "", err
		}

		out, err := json.Marshal(reply)
		if err != nil {
			panic(err)
		}

		return string(out), nil
	})

	shell.Register("help", func(args ...string) (string, error) {
		printHelp(shell)
		return "", nil
	})

	shell.Register("quit", func(args ...string) (string, error) {
		shell.Stop()
		return "", nil
	})

	shell.Start()
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
