package raft

import (
	"context"
	"time"

	"github.com/KYash03/raft-kv/pb"
)

func (n *Node) runLeader() {
	n.broadcastAppendEntries()

	t := time.NewTicker(n.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.broadcastAppendEntries()
		}
	}
}

func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	id := n.cfg.ID
	commit := n.commitIndex
	n.mu.Unlock()

	for _, p := range n.cfg.Peers {
		go n.sendAppendEntriesTo(p, term, id, commit)
	}
}

// heartbeat for now — entries get filled in once replication lands
func (n *Node) sendAppendEntriesTo(peer, term, leaderID, commit uint64) {
	req := &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		LeaderCommit: commit,
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.Heartbeat*2)
	defer cancel()
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	if err != nil || resp == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
	}
}

func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.AppendEntriesResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = n.currentTerm
	}
	// candidate hearing from a same term leader steps down
	if n.state == Candidate {
		n.state = Follower
	}
	n.leaderID = req.LeaderId
	n.lastContact = time.Now()

	// prev log must match
	if req.PrevLogIndex > n.log.lastIndex() {
		resp.ConflictIndex = n.log.lastIndex() + 1
		return resp
	}
	if n.log.termAt(req.PrevLogIndex) != req.PrevLogTerm {
		// rewind past every entry sharing this conflicting term so leader
		// can skip back faster (small optimization on top of basic raft)
		bad := n.log.termAt(req.PrevLogIndex)
		i := req.PrevLogIndex
		for i > 0 && n.log.termAt(i-1) == bad {
			i--
		}
		resp.ConflictIndex = i
		return resp
	}

	// merge entries
	for j, e := range req.Entries {
		idx := req.PrevLogIndex + uint64(j) + 1
		if idx <= n.log.lastIndex() {
			if n.log.termAt(idx) == e.Term {
				continue
			}
			n.log.truncateFrom(idx)
		}
		n.log.entries = append(n.log.entries, LogEntry{
			Term:  e.Term,
			Index: idx,
			Cmd:   e.Cmd,
		})
	}

	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if last := n.log.lastIndex(); newCommit > last {
			newCommit = last
		}
		n.commitIndex = newCommit
	}

	resp.Success = true
	return resp
}
