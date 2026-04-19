package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/KYash03/raft-kv/kv"
	"github.com/KYash03/raft-kv/pb"
	"github.com/KYash03/raft-kv/raft"
)

type Peer struct {
	ID   uint64
	Addr string
}

type Server struct {
	pb.UnimplementedRaftServer
	pb.UnimplementedKVServer

	id    uint64
	addr  string
	peers map[uint64]string // id to addr (no self)

	node  *raft.Node
	store *kv.Store
	tr    *grpcTransport
	gs    *grpc.Server

	wg sync.WaitGroup
}

func New(id uint64, addr string, peers []Peer) *Server {
	pmap := map[uint64]string{}
	var ids []uint64
	for _, p := range peers {
		if p.ID == id {
			continue
		}
		pmap[p.ID] = p.Addr
		ids = append(ids, p.ID)
	}
	tr := newGRPCTransport(pmap)
	cfg := raft.DefaultConfig(id, ids)
	s := &Server{
		id:    id,
		addr:  addr,
		peers: pmap,
		store: kv.New(),
		tr:    tr,
	}
	s.node = raft.NewNode(cfg, tr, nil)
	return s
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.gs = grpc.NewServer()
	pb.RegisterRaftServer(s.gs, s)
	pb.RegisterKVServer(s.gs, s)

	s.wg.Add(2)
	go func() { defer s.wg.Done(); _ = s.gs.Serve(lis) }()
	go func() { defer s.wg.Done(); s.node.Run() }()
	go s.applyLoop()
	return nil
}

func (s *Server) Stop() {
	s.node.Stop()
	if s.gs != nil {
		s.gs.GracefulStop()
	}
	s.tr.close()
	s.wg.Wait()
}

func (s *Server) Node() *raft.Node { return s.node }

func (s *Server) applyLoop() {
	for a := range s.node.ApplyCh() {
		var cmd kv.Command
		if err := json.Unmarshal(a.Cmd, &cmd); err != nil {
			if a.RespCh != nil {
				a.RespCh <- raft.ApplyResult{Err: err}
			}
			continue
		}
		res, err := s.store.Apply(cmd)
		if a.RespCh == nil {
			continue // applied on a follower, no client waiting here
		}
		if err != nil {
			a.RespCh <- raft.ApplyResult{Err: err}
			continue
		}
		b, _ := json.Marshal(res)
		a.RespCh <- raft.ApplyResult{Value: b}
	}
}

// raft RPCs (server side)

func (s *Server) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

func (s *Server) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}

// kv RPCs (server side). every op goes through raft so reads are linearizable.

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.KVResponse, error) {
	_, err := s.runOp(ctx, kv.Command{Op: kv.OpPut, Key: req.Key, Value: req.Value})
	if err != nil {
		return s.errorResp(err), nil
	}
	return &pb.KVResponse{Ok: true}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.KVResponse, error) {
	res, err := s.runOp(ctx, kv.Command{Op: kv.OpGet, Key: req.Key})
	if err != nil {
		return s.errorResp(err), nil
	}
	return &pb.KVResponse{Ok: res.Found, Value: res.Value}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.KVResponse, error) {
	res, err := s.runOp(ctx, kv.Command{Op: kv.OpDel, Key: req.Key})
	if err != nil {
		return s.errorResp(err), nil
	}
	return &pb.KVResponse{Ok: res.Found}, nil
}

func (s *Server) runOp(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	_, ch, err := s.node.Submit(kv.Encode(cmd))
	if err != nil {
		return kv.Result{}, err
	}
	select {
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return kv.Result{}, r.Err
		}
		var out kv.Result
		_ = json.Unmarshal(r.Value, &out)
		return out, nil
	case <-time.After(2 * time.Second):
		return kv.Result{}, fmt.Errorf("commit timeout")
	}
}

func (s *Server) errorResp(err error) *pb.KVResponse {
	resp := &pb.KVResponse{Error: err.Error()}
	if errors.Is(err, raft.ErrNotLeader) {
		if hint := s.peers[s.node.LeaderID()]; hint != "" {
			resp.LeaderHint = hint
		}
	}
	return resp
}
