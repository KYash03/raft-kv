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
	// candidate hearing from a same-term leader needs to step down
	if n.state == Candidate {
		n.state = Follower
	}
	n.leaderID = req.LeaderId
	n.lastContact = time.Now()
	resp.Success = true
	return resp
}
