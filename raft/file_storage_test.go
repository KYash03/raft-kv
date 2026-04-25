package raft

import (
	"reflect"
	"testing"
)

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.SaveHardState(7, 3); err != nil {
		t.Fatal(err)
	}
	entries := []LogEntry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 2, Index: 3, Cmd: []byte("c")},
	}
	if err := s.AppendLog(entries); err != nil {
		t.Fatal(err)
	}

	// reopen
	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	term, voted, got, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 7 || voted != 3 {
		t.Fatalf("hard state, got (%d,%d), want (7,3)", term, voted)
	}
	if !reflect.DeepEqual(got, entries) {
		t.Fatalf("log, got %+v want %+v", got, entries)
	}
}

func TestFileStorageTruncate(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	entries := []LogEntry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 2, Index: 3, Cmd: []byte("c")},
		{Term: 2, Index: 4, Cmd: []byte("d")},
	}
	if err := s.AppendLog(entries); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateLog(3); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog([]LogEntry{{Term: 3, Index: 3, Cmd: []byte("c'")}}); err != nil {
		t.Fatal(err)
	}

	_, _, got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := []LogEntry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 3, Index: 3, Cmd: []byte("c'")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after truncate and append, got %+v want %+v", got, want)
	}
}
