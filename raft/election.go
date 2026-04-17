package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/KYash03/raft-kv/pb"
)

func (n *Node) Run() {
	go n.applier()
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}
		s, _ := n.State()
		switch s {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) electionTimeout() time.Duration {
	min := n.cfg.ElectionMin
	return min + time.Duration(rand.Int63n(int64(n.cfg.ElectionMax-min)))
}

func (n *Node) runFollower() {
	to := n.electionTimeout()
	t := time.NewTimer(to)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.mu.Lock()
			elapsed := time.Since(n.lastContact)
			n.mu.Unlock()
			if elapsed >= to {
				n.mu.Lock()
				n.state = Candidate
				n.mu.Unlock()
				return
			}
			to = n.electionTimeout()
			t.Reset(to)
		}
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.leaderID = 0
	term := n.currentTerm
	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	n.lastContact = time.Now()
	n.logger.Printf("[%d] now candidate (term %d)", n.cfg.ID, term)
	n.mu.Unlock()

	need := n.quorum()
	got := 1 // self

	type res struct {
		resp *pb.RequestVoteResponse
		err  error
	}
	ch := make(chan res, len(n.cfg.Peers))

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionMax)
	defer cancel()

	var wg sync.WaitGroup
	for _, p := range n.cfg.Peers {
		wg.Add(1)
		go func(peer uint64) {
			defer wg.Done()
			r, err := n.transport.RequestVote(ctx, peer, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.cfg.ID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			ch <- res{r, err}
		}(p)
	}
	go func() { wg.Wait(); close(ch) }()

	t := time.NewTimer(n.electionTimeout())
	defer t.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			// timed out, run loop will start a fresh election
			return
		case r, ok := <-ch:
			if !ok {
				ch = nil
				continue
			}
			if r.err != nil || r.resp == nil {
				continue
			}
			n.mu.Lock()
			if n.state != Candidate || n.currentTerm != term {
				n.mu.Unlock()
				return
			}
			if r.resp.Term > n.currentTerm {
				n.becomeFollower(r.resp.Term)
				n.mu.Unlock()
				return
			}
			if r.resp.VoteGranted {
				got++
				if got >= need {
					n.becomeLeader()
					n.mu.Unlock()
					return
				}
			}
			n.mu.Unlock()
		}
	}
}

// caller holds mu
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.cfg.ID
	last := n.log.lastIndex()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.logger.Printf("[%d] now leader (term %d)", n.cfg.ID, n.currentTerm)
}

func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.RequestVoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = n.currentTerm
	}
	if (n.votedFor == 0 || n.votedFor == req.CandidateId) &&
		n.log.upToDate(req.LastLogTerm, req.LastLogIndex) {
		n.votedFor = req.CandidateId
		n.lastContact = time.Now()
		resp.VoteGranted = true
	}
	return resp
}
