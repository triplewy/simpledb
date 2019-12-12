package ycsb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/user"
	"path"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type simpleDBCreator struct {
}

type simpleDBClient struct {
	client pb.SimpleDbClient
}

type contextKey string

const stateKey = contextKey("simpledb")

type simpleDBState struct {
}

func (c simpleDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	client, err := connect("localhost:8888")
	if err != nil {
		return nil, err
	}
	return &simpleDBClient{
		client: client,
	}, nil
}

func connect(addr string) (pb.SimpleDbClient, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, err
	}
	cert := path.Join(usr.HomeDir, ".ssl/cert.pem")
	creds, err := credentials.NewClientTLSFromFile(cert, "")
	if err != nil {
		log.Fatalf("could not create credentials: %v", err)
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewSimpleDbClient(conn), nil
}

func (c *simpleDBClient) Close() error {
	return nil
}

func (c *simpleDBClient) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (c *simpleDBClient) CleanupThread(_ context.Context) {
}

func (c *simpleDBClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	entry, err := c.client.ReadRPC(ctx, &pb.ReadMsg{Key: table + key, Fields: fields})
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte)
	for _, field := range entry.GetValues() {
		result[field.GetName()] = field.GetValue()
	}
	return result, nil
}

func (c *simpleDBClient) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	entries, err := c.client.ScanRPC(ctx, &pb.ReadMsg{Key: table + startKey, Fields: fields})
	if err != nil {
		return nil, err
	}
	result := []map[string][]byte{}
	for _, entry := range entries.GetEntries() {
		fields := make(map[string][]byte)
		for _, field := range entry.GetValues() {
			fields[field.GetName()] = field.GetValue()
		}
		result = append(result, fields)
	}
	return result, nil
}

func (c *simpleDBClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	fields := []*pb.Field{}
	for name, value := range values {
		fields = append(fields, &pb.Field{Name: name, Value: value})
	}
	reply, err := c.client.UpdateRPC(ctx, &pb.Entry{Key: table + key, Values: fields})
	if err != nil {
		fmt.Println(err)
		return err
	}
	if !reply.GetOk() {
		return errors.New("Error updating DB")
	}
	return nil
}

func (c *simpleDBClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	fields := []*pb.Field{}
	for name, value := range values {
		fields = append(fields, &pb.Field{Name: name, Value: value})
	}
	reply, err := c.client.InsertRPC(ctx, &pb.Entry{Key: table + key, Values: fields})
	if err != nil {
		return err
	}
	if !reply.GetOk() {
		return errors.New("Error updating DB")
	}
	return nil
}

func (c *simpleDBClient) Delete(ctx context.Context, table string, key string) error {
	reply, err := c.client.DeleteRPC(ctx, &pb.KeyMsg{Key: table + key})
	if err != nil {
		return err
	}
	if !reply.GetOk() {
		return errors.New("Error updating DB")
	}
	return nil
}

func init() {
	ycsb.RegisterDBCreator("simpledb", simpleDBCreator{})
}
