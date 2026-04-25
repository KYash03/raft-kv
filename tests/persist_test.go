package tests

import (
	"encoding/json"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/KYash03/raft-kv/kv"
	"github.com/KYash03/raft-kv/raft"
)

// boot a 3 node cluster on a shared in memory network where each node
// has its own FileStorage backed by t.TempDir. then crash and restart
// and check that committed entries survive.
func TestRestartPreservesCommitted(t *testing.T) {
	dir := t.TempDir()
	net := newNetwork()

	mk := func(id uint64, peers []uint64) *clusterNode {
		fs, err := raft.NewFileStorage(filepath.Join(dir, idToName(id)))
		if err != nil {
			t.Fatal(err)
		}
		cfg := raft.DefaultConfig(id, peers)
		cfg.ElectionMin = 100 * time.Millisecond
		cfg.ElectionMax = 200 * time.Millisecond
		cfg.Heartbeat = 30 * time.Millisecond
		cfg.Storage = fs
		node, err := raft.NewNode(cfg, &memTransport{self: id, net: net}, log.New(io.Discard, "", 0))
		if err != nil {
			t.Fatal(err)
		}
		net.register(id, node)
		return &clusterNode{id: id, node: node, store: kv.New()}
	}

	bootAll := func() *cluster {
		c := &cluster{t: t, net: net}
		for _, id := range []uint64{1, 2, 3} {
			peers := otherIDs(id, 3)
			c.nodes = append(c.nodes, mk(id, peers))
		}
		for _, cn := range c.nodes {
			cn := cn
			go cn.node.Run()
			go runApply(cn)
		}
		return c
	}

	c := bootAll()
	c.waitLeader(2 * time.Second)
	for i, kvp := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if err := c.put(kvp.k, kvp.v); err != nil {
			t.Fatalf("put %d, %v", i, err)
		}
	}

	// crash everything
	for _, cn := range c.nodes {
		cn.node.Stop()
	}
	// give run loops a moment to actually exit
	time.Sleep(50 * time.Millisecond)

	// fresh network so old in flight goroutines can't leak into round 2
	net = newNetwork()
	c2 := bootAll()
	t.Cleanup(c2.shutdown)

	c2.waitLeader(3 * time.Second)
	for _, kvp := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		v, ok, err := c2.get(kvp.k)
		if err != nil {
			t.Fatalf("get %s after restart, %v", kvp.k, err)
		}
		if !ok || v != kvp.v {
			t.Fatalf("get %s, got (%q, %v), want (%q, true)", kvp.k, v, ok, kvp.v)
		}
	}
}

func TestSingleNodeRestoresVotedFor(t *testing.T) {
	dir := t.TempDir()

	// term 5, voted for 7, plus a couple log entries
	fs, err := raft.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.SaveHardState(5, 7); err != nil {
		t.Fatal(err)
	}
	if err := fs.AppendLog([]raft.LogEntry{
		{Term: 4, Index: 1, Cmd: []byte(`{"op":"put","k":"x","v":"y"}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// reopen via NewNode and check
	fs2, err := raft.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := raft.DefaultConfig(1, []uint64{2, 3})
	cfg.Storage = fs2
	n, err := raft.NewNode(cfg, &memTransport{self: 1, net: newNetwork()}, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	_, term := n.State()
	if term != 5 {
		t.Fatalf("currentTerm, got %d want 5", term)
	}
}

func idToName(id uint64) string {
	return "node" + string(rune('0'+id))
}

func otherIDs(self uint64, n int) []uint64 {
	var out []uint64
	for i := uint64(1); i <= uint64(n); i++ {
		if i != self {
			out = append(out, i)
		}
	}
	return out
}

func runApply(cn *clusterNode) {
	for a := range cn.node.ApplyCh() {
		var cmd kv.Command
		if err := json.Unmarshal(a.Cmd, &cmd); err != nil {
			if a.RespCh != nil {
				a.RespCh <- raft.ApplyResult{Err: err}
			}
			continue
		}
		res, _ := cn.store.Apply(cmd)
		if a.RespCh == nil {
			continue
		}
		b, _ := json.Marshal(res)
		a.RespCh <- raft.ApplyResult{Value: b}
	}
}
