package raft

type LogEntry struct {
	Term  uint64
	Index uint64
	Cmd   []byte
}

// log[0] is a sentinel so prevLogIndex=0 always matches
type Log struct {
	entries []LogEntry
}

func newLog() *Log {
	return &Log{entries: []LogEntry{{}}}
}

func (l *Log) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }
func (l *Log) lastTerm() uint64  { return l.entries[len(l.entries)-1].Term }

func (l *Log) termAt(i uint64) uint64 {
	if i >= uint64(len(l.entries)) {
		return 0
	}
	return l.entries[i].Term
}

func (l *Log) entryAt(i uint64) LogEntry { return l.entries[i] }

func (l *Log) slice(from, to uint64) []LogEntry {
	if from >= uint64(len(l.entries)) {
		return nil
	}
	if to > uint64(len(l.entries)) {
		to = uint64(len(l.entries))
	}
	out := make([]LogEntry, to-from)
	copy(out, l.entries[from:to])
	return out
}

func (l *Log) append(cmd []byte, term uint64) LogEntry {
	e := LogEntry{Term: term, Index: l.lastIndex() + 1, Cmd: cmd}
	l.entries = append(l.entries, e)
	return e
}

func (l *Log) truncateFrom(i uint64) {
	if i < uint64(len(l.entries)) {
		l.entries = l.entries[:i]
	}
}

// §5.4.1
func (l *Log) upToDate(lastTerm, lastIdx uint64) bool {
	if lastTerm != l.lastTerm() {
		return lastTerm > l.lastTerm()
	}
	return lastIdx >= l.lastIndex()
}
