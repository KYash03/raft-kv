package raft

import (
	"errors"
	"time"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrShutdown  = errors.New("node stopped")
)

// Submit hands a cmd to the leader and returns a channel that fires when
// the entry commits. ErrNotLeader if called on a follower.
func (n *Node) Submit(cmd []byte) (uint64, <-chan ApplyResult, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, nil, ErrNotLeader
	}
	e := LogEntry{Term: n.currentTerm, Index: n.log.lastIndex() + 1, Cmd: cmd}
	if err := n.storage.AppendLog([]LogEntry{e}); err != nil {
		n.mu.Unlock()
		return 0, nil, err
	}
	n.log.entries = append(n.log.entries, e)
	ch := make(chan ApplyResult, 1)
	n.pending[e.Index] = ch
	term := n.currentTerm
	n.mu.Unlock()

	// kick replication right away rather than waiting for next tick
	for _, p := range n.cfg.Peers {
		go n.replicateTo(p, term)
	}
	return e.Index, ch, nil
}

// runs alongside Run(). pumps committed entries out to the state machine.
func (n *Node) applier() {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			n.drainPending(ErrShutdown)
			close(n.applyCh)
			return
		case <-t.C:
			n.dispatchCommitted()
		}
	}
}

func (n *Node) dispatchCommitted() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		n.lastApplied++
		idx := n.lastApplied
		entry := n.log.entryAt(idx)
		ch := n.pending[idx]
		delete(n.pending, idx)
		n.mu.Unlock()

		select {
		case n.applyCh <- Apply{Index: idx, Cmd: entry.Cmd, RespCh: ch}:
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) drainPending(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, ch := range n.pending {
		ch <- ApplyResult{Err: err}
		delete(n.pending, i)
	}
}
