package raft

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/KYash03/raft-kv/pb"
)

// figure 8 from the raft paper. a leader cannot mark an entry from a
// previous term committed just because it sits on a majority. it must
// also commit something from its own term first; older entries then
// commit indirectly via the log matching property.
//
// this test pokes the leader state directly rather than driving real
// rpcs, since the partition and restart sequencing in figure 8 is hard
// to set up deterministically.
func TestFigure8_NoCommitOfPriorTermByMajority(t *testing.T) {
	n := newTestLeader(t, /*id=*/ 1, /*peers=*/ []uint64{2, 3}, /*term=*/ 3)

	// log entries.
	//   index 1 from term 2 (left over from a previous leader)
	//   index 2 from term 3 (we just accepted it as the term 3 leader)
	n.log.entries = append(n.log.entries,
		LogEntry{Term: 2, Index: 1, Cmd: []byte("old")},
		LogEntry{Term: 3, Index: 2, Cmd: []byte("new")},
	)

	// pretend a majority has the OLD entry but not the new one
	n.matchIndex[2] = 1
	n.matchIndex[3] = 1
	n.maybeAdvanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("commit advanced to %d on a prior term entry, should stay 0", n.commitIndex)
	}

	// now a majority gets the CURRENT term entry too
	n.matchIndex[2] = 2
	// node 3 still only has index 1, so we have 2 of 3 with index >= 2
	// (self plus node 2). that's a majority.
	n.maybeAdvanceCommit()
	if n.commitIndex != 2 {
		t.Fatalf("commit didn't advance to 2 after current term entry on majority, got %d", n.commitIndex)
	}
	// index 1 is committed indirectly (log matching), not by counting replicas
}

// also worth nailing down. same setup but the entry on majority is from
// the leader's current term. should commit it directly.
func TestCommitAdvancesOnCurrentTermMajority(t *testing.T) {
	n := newTestLeader(t, 1, []uint64{2, 3}, 5)
	n.log.entries = append(n.log.entries,
		LogEntry{Term: 5, Index: 1, Cmd: []byte("a")},
	)
	n.matchIndex[2] = 1
	n.matchIndex[3] = 0
	n.maybeAdvanceCommit()
	if n.commitIndex != 1 {
		t.Fatalf("commit, got %d want 1", n.commitIndex)
	}
}

// noop transport for the unit tests above
type nopTransport struct{}

func (nopTransport) RequestVote(_ context.Context, _ uint64, _ *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return nil, nil
}
func (nopTransport) AppendEntries(_ context.Context, _ uint64, _ *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return nil, nil
}

func newTestLeader(t *testing.T, id uint64, peers []uint64, term uint64) *Node {
	t.Helper()
	cfg := DefaultConfig(id, peers)
	cfg.Storage = NewMemoryStorage()
	n, err := NewNode(cfg, nopTransport{}, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	n.currentTerm = term
	n.state = Leader
	for _, p := range peers {
		n.nextIndex[p] = 1
		n.matchIndex[p] = 0
	}
	return n
}
