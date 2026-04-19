package server

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/KYash03/raft-kv/pb"
)

type grpcTransport struct {
	addrs map[uint64]string

	mu    sync.Mutex
	conns map[uint64]*grpc.ClientConn
}

func newGRPCTransport(addrs map[uint64]string) *grpcTransport {
	return &grpcTransport{addrs: addrs, conns: map[uint64]*grpc.ClientConn{}}
}

func (t *grpcTransport) client(peer uint64) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return pb.NewRaftClient(c), nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", peer)
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[peer] = c
	return pb.NewRaftClient(c), nil
}

func (t *grpcTransport) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	t.conns = nil
}

func (t *grpcTransport) RequestVote(ctx context.Context, peer uint64, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *grpcTransport) AppendEntries(ctx context.Context, peer uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}
