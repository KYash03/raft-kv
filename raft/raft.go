package raft

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/KYash03/raft-kv/pb"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "?"
}

type Apply struct {
	Index  uint64
	Cmd    []byte
	RespCh chan ApplyResult
}

type ApplyResult struct {
	Value []byte
	Err   error
}

// outbound RPCs to peers. gRPC in prod, in memory for tests.
type Transport interface {
	RequestVote(ctx context.Context, peer uint64, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

type Config struct {
	ID          uint64
	Peers       []uint64 // not including self
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
	Storage     Storage // defaults to in memory if nil
}

func DefaultConfig(id uint64, peers []uint64) Config {
	return Config{
		ID:          id,
		Peers:       peers,
		ElectionMin: 150 * time.Millisecond,
		ElectionMax: 300 * time.Millisecond,
		Heartbeat:   50 * time.Millisecond,
	}
}

type Node struct {
	cfg       Config
	transport Transport
	storage   Storage

	mu sync.Mutex

	state       State
	currentTerm uint64
	votedFor    uint64 // 0 = nobody
	log         *Log

	commitIndex uint64
	lastApplied uint64

	// leader only
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64
	pending    map[uint64]chan ApplyResult

	lastContact time.Time
	leaderID    uint64

	applyCh  chan Apply
	stopCh   chan struct{}
	stopOnce sync.Once

	logger *log.Logger
}

func NewNode(cfg Config, t Transport, logger *log.Logger) *Node {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}
	return &Node{
		cfg:         cfg,
		transport:   t,
		storage:     cfg.Storage,
		state:       Follower,
		log:         newLog(),
		nextIndex:   make(map[uint64]uint64),
		matchIndex:  make(map[uint64]uint64),
		pending:     make(map[uint64]chan ApplyResult),
		lastContact: time.Now(),
		applyCh:     make(chan Apply, 64),
		stopCh:      make(chan struct{}),
		logger:      logger,
	}
}

func (n *Node) ApplyCh() <-chan Apply { return n.applyCh }

func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

func (n *Node) State() (State, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

func (n *Node) LeaderID() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *Node) quorum() int { return (len(n.cfg.Peers)+1)/2 + 1 }

// caller holds mu. when term goes up we MUST acknowledge (the incoming RPC
// already saw the higher term), so persist is best effort here. a failure
// gets logged and the in memory bump still happens.
func (n *Node) becomeFollower(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = 0
		if err := n.storage.SaveHardState(n.currentTerm, n.votedFor); err != nil {
			n.logger.Printf("[%d] persist on step down, %v", n.cfg.ID, err)
		}
	}
	if n.state != Follower {
		n.logger.Printf("[%d] now follower (term %d)", n.cfg.ID, n.currentTerm)
	}
	n.state = Follower
}
