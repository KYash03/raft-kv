package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/KYash03/raft-kv/kv"
	"github.com/KYash03/raft-kv/pb"
	"github.com/KYash03/raft-kv/raft"
)

// in memory network. each node gets its own transport that routes
// RPCs through here, so tests can pause/drop traffic between any pair.
type network struct {
	mu      sync.Mutex
	nodes   map[uint64]*raft.Node
	blocked map[uint64]map[uint64]bool // src to dst
	down    map[uint64]bool            // pretend node is offline
}

func newNetwork() *network {
	return &network{
		nodes:   map[uint64]*raft.Node{},
		blocked: map[uint64]map[uint64]bool{},
		down:    map[uint64]bool{},
	}
}

func (n *network) register(id uint64, node *raft.Node) {
	n.mu.Lock()
	n.nodes[id] = node
	n.mu.Unlock()
}

func (n *network) drops(src, dst uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[src] || n.down[dst] {
		return true
	}
	if m, ok := n.blocked[src]; ok && m[dst] {
		return true
	}
	if m, ok := n.blocked[dst]; ok && m[src] {
		return true
	}
	return false
}

func (n *network) target(id uint64) *raft.Node {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[id]
}

func (n *network) partition(a, b uint64, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.blocked[a] == nil {
		n.blocked[a] = map[uint64]bool{}
	}
	if n.blocked[b] == nil {
		n.blocked[b] = map[uint64]bool{}
	}
	n.blocked[a][b] = on
	n.blocked[b][a] = on
}

func (n *network) setDown(id uint64, down bool) {
	n.mu.Lock()
	n.down[id] = down
	n.mu.Unlock()
}

type memTransport struct {
	self uint64
	net  *network
}

func (t *memTransport) RequestVote(ctx context.Context, peer uint64, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if t.net.drops(t.self, peer) {
		return nil, errors.New("partitioned")
	}
	n := t.net.target(peer)
	if n == nil {
		return nil, errors.New("no peer")
	}
	return n.HandleRequestVote(req), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, peer uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if t.net.drops(t.self, peer) {
		return nil, errors.New("partitioned")
	}
	n := t.net.target(peer)
	if n == nil {
		return nil, errors.New("no peer")
	}
	return n.HandleAppendEntries(req), nil
}

type clusterNode struct {
	id      uint64
	node    *raft.Node
	store   *kv.Store
	stopped bool
}

type cluster struct {
	t     *testing.T
	net   *network
	nodes []*clusterNode
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	net := newNetwork()
	c := &cluster{t: t, net: net}

	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
	}

	for _, id := range ids {
		peers := make([]uint64, 0, n-1)
		for _, j := range ids {
			if j != id {
				peers = append(peers, j)
			}
		}
		// quiet logger so tests aren't a wall of noise
		logger := log.New(io.Discard, "", 0)
		cfg := raft.DefaultConfig(id, peers)
		cfg.ElectionMin = 100 * time.Millisecond
		cfg.ElectionMax = 200 * time.Millisecond
		cfg.Heartbeat = 30 * time.Millisecond
		node, err := raft.NewNode(cfg, &memTransport{self: id, net: net}, logger)
		if err != nil {
			t.Fatalf("new node %d, %v", id, err)
		}
		net.register(id, node)
		c.nodes = append(c.nodes, &clusterNode{id: id, node: node, store: kv.New()})
	}

	for _, cn := range c.nodes {
		cn := cn
		go cn.node.Run()
		go func() {
			for a := range cn.node.ApplyCh() {
				var cmd kv.Command
				if err := json.Unmarshal(a.Cmd, &cmd); err != nil {
					if a.RespCh != nil {
						a.RespCh <- raft.ApplyResult{Err: err}
					}
					continue
				}
				res, err := cn.store.Apply(cmd)
				if a.RespCh == nil {
					continue
				}
				if err != nil {
					a.RespCh <- raft.ApplyResult{Err: err}
					continue
				}
				b, _ := json.Marshal(res)
				a.RespCh <- raft.ApplyResult{Value: b}
			}
		}()
	}

	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) shutdown() {
	for _, cn := range c.nodes {
		if !cn.stopped {
			cn.node.Stop()
		}
	}
}

// kill takes a node out of the test "world". drops its network and
// stops the node loops. note that the underlying raft.Node's state
// field doesn't get reset, so without the stopped flag below we'd
// still see it as "leader" when iterating.
func (c *cluster) kill(cn *clusterNode) {
	c.net.setDown(cn.id, true)
	cn.node.Stop()
	cn.stopped = true
}

func (c *cluster) leader() (*clusterNode, bool) {
	for _, cn := range c.nodes {
		if cn.stopped {
			continue
		}
		s, _ := cn.node.State()
		if s == raft.Leader {
			return cn, true
		}
	}
	return nil, false
}

// waitLeader blocks until exactly one live node is the leader.
func (c *cluster) waitLeader(d time.Duration) *clusterNode {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		var ldr *clusterNode
		count := 0
		for _, cn := range c.nodes {
			if cn.stopped {
				continue
			}
			s, _ := cn.node.State()
			if s == raft.Leader {
				ldr = cn
				count++
			}
		}
		if count == 1 {
			return ldr
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no unique leader after %s", d)
	return nil
}

func (c *cluster) put(key, val string) error {
	ldr, ok := c.leader()
	if !ok {
		return fmt.Errorf("no leader")
	}
	_, ch, err := ldr.node.Submit(kv.Encode(kv.Command{Op: kv.OpPut, Key: key, Value: val}))
	if err != nil {
		return err
	}
	select {
	case r := <-ch:
		return r.Err
	case <-time.After(2 * time.Second):
		return fmt.Errorf("put commit timed out")
	}
}

func encodePut(k, v string) []byte {
	return kv.Encode(kv.Command{Op: kv.OpPut, Key: k, Value: v})
}

func (c *cluster) get(key string) (string, bool, error) {
	ldr, ok := c.leader()
	if !ok {
		return "", false, fmt.Errorf("no leader")
	}
	_, ch, err := ldr.node.Submit(kv.Encode(kv.Command{Op: kv.OpGet, Key: key}))
	if err != nil {
		return "", false, err
	}
	select {
	case r := <-ch:
		if r.Err != nil {
			return "", false, r.Err
		}
		var res kv.Result
		_ = json.Unmarshal(r.Value, &res)
		return res.Value, res.Found, nil
	case <-time.After(2 * time.Second):
		return "", false, fmt.Errorf("get commit timed out")
	}
}
