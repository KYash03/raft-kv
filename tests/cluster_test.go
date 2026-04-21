package tests

import (
	"testing"
	"time"
)

func TestElectsLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(2 * time.Second)
}

func TestPutGet(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(2 * time.Second)

	if err := c.put("hello", "world"); err != nil {
		t.Fatalf("put, %v", err)
	}
	v, ok, err := c.get("hello")
	if err != nil {
		t.Fatalf("get, %v", err)
	}
	if !ok || v != "world" {
		t.Fatalf("got (%q, %v), want (\"world\", true)", v, ok)
	}
}

func TestLeaderFailoverOnHeartbeatStop(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader(2 * time.Second)

	if err := c.put("k", "v1"); err != nil {
		t.Fatalf("put before failover, %v", err)
	}

	// take old leader offline
	c.net.setDown(old.id, true)
	old.node.Stop()

	// wait for someone else to win an election
	deadline := time.Now().Add(5 * time.Second)
	var fresh *clusterNode
	for time.Now().Before(deadline) {
		l, ok := c.leader()
		if ok && l.id != old.id {
			fresh = l
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if fresh == nil {
		t.Fatal("no new leader elected after stopping old one")
	}

	v, ok, err := c.get("k")
	if err != nil {
		t.Fatalf("get after failover, %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("got (%q, %v), want (\"v1\", true)", v, ok)
	}
}
