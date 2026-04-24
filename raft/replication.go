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
	n.mu.Unlock()
	for _, p := range n.cfg.Peers {
		go n.replicateTo(p, term)
	}
}

func (n *Node) replicateTo(peer, term uint64) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevIdx := next - 1
	prevTerm := n.log.termAt(prevIdx)
	entries := n.log.slice(next, n.log.lastIndex()+1)
	commit := n.commitIndex
	n.mu.Unlock()

	pbEntries := make([]*pb.LogEntry, len(entries))
	for i := range entries {
		pbEntries[i] = &pb.LogEntry{
			Term:  entries[i].Term,
			Index: entries[i].Index,
			Cmd:   entries[i].Cmd,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.Heartbeat*4)
	defer cancel()
	resp, err := n.transport.AppendEntries(ctx, peer, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      pbEntries,
		LeaderCommit: commit,
	})
	if err != nil || resp == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {
		match := prevIdx + uint64(len(entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = match + 1
		n.maybeAdvanceCommit()
		return
	}

	// rejected. back nextIndex up using the conflict hint.
	if resp.ConflictIndex > 0 && resp.ConflictIndex < n.nextIndex[peer] {
		n.nextIndex[peer] = resp.ConflictIndex
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
}

// caller holds mu. §5.4.2 says only commit entries from current term directly.
func (n *Node) maybeAdvanceCommit() {
	for N := n.log.lastIndex(); N > n.commitIndex; N-- {
		if n.log.termAt(N) != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = N
			return
		}
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

	// figure 2, AE step 5. min(leaderCommit, index of last new entry).
	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNew {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}

	resp.Success = true
	return resp
}
