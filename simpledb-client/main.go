package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
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
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <node-address> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func printHelp(shell *ishell.Shell) {
	shell.Println("Commands:")
	shell.Println(" - help                    Prints this help message")
	shell.Println(" - read <key> <fields>     Read")
	shell.Println(" - scan <key> <fields>     Scan")
	shell.Println(" - update <key> <entry>    Update")
	shell.Println(" - insert <key> <entry>    Insert")
	shell.Println(" - delete <key>            Delete")
	shell.Println(" - quit                    Quit CLI")
}

func main() {
	flag.Parse()
	c := connect(addr)

	shell := ishell.New()
	printHelp(shell)

	shell.Register("help", func(args ...string) (string, error) {
		printHelp(shell)
		return "", nil
	})

	shell.Register("read", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if len(args) != 2 {
			return "", errors.New("Args should be length 2")
		}
		fields := strings.Split(args[1], ",")
		reply, err := c.ReadRPC(ctx, &pb.ReadMsg{Key: args[0], Fields: fields})
		if err != nil {
			return "", err
		}
		out, err := json.Marshal(reply)
		if err != nil {
			return "", err
		}
		return string(out), nil
	})

	shell.Register("scan", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if len(args) != 2 {
			return "", errors.New("Args should be length 2")
		}
		fields := strings.Split(args[1], ",")
		reply, err := c.ScanRPC(ctx, &pb.ReadMsg{Key: args[0], Fields: fields})
		if err != nil {
			return "", err
		}
		out, err := json.Marshal(reply)
		if err != nil {
			panic(err)
		}
		return string(out), nil
	})

	shell.Register("update", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if len(args) != 2 {
			return "", errors.New("Args should be length 2")
		}
		fields := strings.Split(args[1], ",")
		values := []*pb.Field{}
		for _, field := range fields {
			value := strings.Split(field, ":")
			values = append(values, &pb.Field{
				Name:  value[0],
				Value: []byte(value[1]),
			})
		}
		reply, err := c.UpdateRPC(ctx, &pb.Entry{Key: args[0], Values: values})
		if err != nil {
			return "", err
		}
		out, err := json.Marshal(reply)
		if err != nil {
			panic(err)
		}
		return string(out), nil
	})

	shell.Register("insert", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if len(args) != 2 {
			return "", errors.New("Args should be length 2")
		}
		fields := strings.Split(args[1], ",")
		values := []*pb.Field{}
		for _, field := range fields {
			value := strings.Split(field, ":")
			values = append(values, &pb.Field{
				Name:  value[0],
				Value: []byte(value[1]),
			})
		}
		reply, err := c.InsertRPC(ctx, &pb.Entry{Key: args[0], Values: values})
		if err != nil {
			return "", err
		}
		out, err := json.Marshal(reply)
		if err != nil {
			panic(err)
		}
		return string(out), nil
	})

	shell.Register("delete", func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		reply, err := c.DeleteRPC(ctx, &pb.KeyMsg{Key: args[0]})
		if err != nil {
			return "", err
		}
		out, err := json.Marshal(reply)
		if err != nil {
			panic(err)
		}
		return string(out), nil
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
