package kv

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Op string

const (
	OpPut Op = "put"
	OpGet Op = "get"
	OpDel Op = "del"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"k"`
	Value string `json:"v,omitempty"`
}

type Result struct {
	Value string `json:"v,omitempty"`
	Found bool   `json:"f,omitempty"`
}

func Encode(c Command) []byte {
	b, _ := json.Marshal(c)
	return b
}

func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

type Store struct {
	mu sync.RWMutex
	m  map[string]string
}

func New() *Store { return &Store{m: map[string]string{}} }

func (s *Store) Apply(cmd Command) (Result, error) {
	switch cmd.Op {
	case OpPut:
		s.mu.Lock()
		s.m[cmd.Key] = cmd.Value
		s.mu.Unlock()
		return Result{}, nil
	case OpGet:
		s.mu.RLock()
		v, ok := s.m[cmd.Key]
		s.mu.RUnlock()
		return Result{Value: v, Found: ok}, nil
	case OpDel:
		s.mu.Lock()
		_, ok := s.m[cmd.Key]
		delete(s.m, cmd.Key)
		s.mu.Unlock()
		return Result{Found: ok}, nil
	}
	return Result{}, fmt.Errorf("unknown op %q", cmd.Op)
}
