package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// disk layout per node.
//   <dir>/state, JSON {term, votedFor}, atomic rewrite
//   <dir>/log, length prefixed JSON entries, append only,
//              rewritten on truncate
type FileStorage struct {
	mu        sync.Mutex
	statePath string
	logPath   string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{
		statePath: filepath.Join(dir, "state"),
		logPath:   filepath.Join(dir, "log"),
	}, nil
}

type hardState struct {
	Term     uint64 `json:"term"`
	VotedFor uint64 `json:"votedFor"`
}

func (s *FileStorage) SaveHardState(term, votedFor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _ := json.Marshal(hardState{term, votedFor})
	return atomicWriteFile(s.statePath, data)
}

func (s *FileStorage) AppendLog(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := writeEntry(f, e); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (s *FileStorage) TruncateLog(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := readLog(s.logPath)
	if err != nil {
		return err
	}
	keep := all[:0]
	for _, e := range all {
		if e.Index >= from {
			break
		}
		keep = append(keep, e)
	}
	return rewriteLog(s.logPath, keep)
}

func (s *FileStorage) Load() (uint64, uint64, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var term, votedFor uint64
	data, err := os.ReadFile(s.statePath)
	if err == nil {
		var hs hardState
		if err := json.Unmarshal(data, &hs); err != nil {
			return 0, 0, nil, fmt.Errorf("decode state, %w", err)
		}
		term, votedFor = hs.Term, hs.VotedFor
	} else if !os.IsNotExist(err) {
		return 0, 0, nil, err
	}

	entries, err := readLog(s.logPath)
	if err != nil {
		return 0, 0, nil, err
	}
	return term, votedFor, entries, nil
}

func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeEntry(w io.Writer, e LogEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readLog(path string) ([]LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []LogEntry
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if err == io.EOF {
				return out, nil
			}
			// torn write. drop the tail rather than refuse to start.
			return out, nil
		}
		l := binary.BigEndian.Uint32(hdr[:])
		buf := make([]byte, l)
		if _, err := io.ReadFull(f, buf); err != nil {
			// torn write again, same deal
			return out, nil
		}
		var e LogEntry
		if err := json.Unmarshal(buf, &e); err != nil {
			return nil, fmt.Errorf("decode entry, %w", err)
		}
		out = append(out, e)
	}
}

func rewriteLog(path string, entries []LogEntry) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := writeEntry(f, e); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
