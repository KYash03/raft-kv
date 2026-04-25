package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/KYash03/raft-kv/server"
)

func main() {
	id := flag.Uint64("id", 0, "this node's id (must be > 0)")
	addr := flag.String("addr", "", "listen address (passed to net.Listen)")
	peerStr := flag.String("peers", "", "comma sep peer list of id@addr, include self")
	dataDir := flag.String("data", "", "directory for persistent state (empty for in memory only)")
	flag.Parse()

	if *id == 0 || *addr == "" || *peerStr == "" {
		flag.Usage()
		os.Exit(2)
	}

	peers, err := parsePeers(*peerStr)
	if err != nil {
		log.Fatalf("bad peers value, %v", err)
	}

	s, err := server.New(*id, *addr, *dataDir, peers)
	if err != nil {
		log.Fatalf("new server, %v", err)
	}
	if err := s.Start(); err != nil {
		log.Fatalf("start, %v", err)
	}
	log.Printf("node %d listening on %s", *id, *addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	s.Stop()
}

func parsePeers(s string) ([]server.Peer, error) {
	var out []server.Peer
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		at := strings.IndexByte(p, '@')
		if at < 0 {
			return nil, fmt.Errorf("missing @ in %q", p)
		}
		id, err := strconv.ParseUint(p[:at], 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, server.Peer{ID: id, Addr: p[at+1:]})
	}
	return out, nil
}
