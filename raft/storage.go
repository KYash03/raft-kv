package raft

import "sync"

// figure 2 says "Updated on stable storage before responding to RPCs". in
// practice that means before HandleRequestVote returns true, before
// HandleAppendEntries returns success, and before Submit returns to a
// caller, the new state has to be on disk.
type Storage interface {
	SaveHardState(term, votedFor uint64) error
	AppendLog(entries []LogEntry) error
	TruncateLog(from uint64) error
	Load() (term, votedFor uint64, entries []LogEntry, err error)
}

type MemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor uint64
	entries  []LogEntry
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) SaveHardState(term, votedFor uint64) error {
	m.mu.Lock()
	m.term, m.votedFor = term, votedFor
	m.mu.Unlock()
	return nil
}

func (m *MemoryStorage) AppendLog(entries []LogEntry) error {
	m.mu.Lock()
	m.entries = append(m.entries, entries...)
	m.mu.Unlock()
	return nil
}

func (m *MemoryStorage) TruncateLog(from uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	keep := m.entries[:0]
	for _, e := range m.entries {
		if e.Index >= from {
			break
		}
		keep = append(keep, e)
	}
	m.entries = keep
	return nil
}

func (m *MemoryStorage) Load() (uint64, uint64, []LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]LogEntry, len(m.entries))
	copy(cp, m.entries)
	return m.term, m.votedFor, cp, nil
}
