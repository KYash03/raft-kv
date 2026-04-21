package tests

import (
	"testing"
	"time"
)

func TestMinorityFailureMakesProgress(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(2 * time.Second)

	// kill 2 of 5. quorum is still 3 so we should still commit.
	killed := 0
	for _, cn := range c.nodes {
		s, _ := cn.node.State()
		if s != 0 /* Follower */ {
			continue
		}
		c.net.setDown(cn.id, true)
		cn.node.Stop()
		killed++
		if killed == 2 {
			break
		}
	}

	if err := c.put("a", "1"); err != nil {
		t.Fatalf("put, %v", err)
	}
	v, ok, err := c.get("a")
	if err != nil || !ok || v != "1" {
		t.Fatalf("got (%q, %v, %v), want (\"1\", true, nil)", v, ok, err)
	}
}

func TestMajorityFailureBlocks(t *testing.T) {
	c := newCluster(t, 3)
	ldr := c.waitLeader(2 * time.Second)

	// take down both followers. leader is alone, no quorum so no commit.
	for _, cn := range c.nodes {
		if cn.id == ldr.id {
			continue
		}
		c.net.setDown(cn.id, true)
		cn.node.Stop()
	}

	// Submit will succeed (it's queued in the log) but commit won't.
	// our put helper has a 2s timeout so we expect the timeout error.
	err := c.put("blocked", "yes")
	if err == nil {
		t.Fatal("expected put to time out without quorum")
	}
}

func TestPartitionHealing(t *testing.T) {
	c := newCluster(t, 5)
	old := c.waitLeader(3 * time.Second)

	if err := c.put("k", "before"); err != nil {
		t.Fatalf("put before partition, %v", err)
	}

	// isolate the leader from the other 4
	for _, cn := range c.nodes {
		if cn.id == old.id {
			continue
		}
		c.net.partition(old.id, cn.id, true)
	}

	// majority side should elect a fresh leader
	deadline := time.Now().Add(4 * time.Second)
	var fresh *clusterNode
	for time.Now().Before(deadline) {
		for _, cn := range c.nodes {
			if cn.id == old.id {
				continue
			}
			s, _ := cn.node.State()
			if s == 2 /* Leader */ {
				fresh = cn
				break
			}
		}
		if fresh != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if fresh == nil {
		t.Fatal("majority side never elected a leader")
	}

	// majority can commit something new
	_, ch, err := fresh.node.Submit(encodePut("k", "after"))
	if err != nil {
		t.Fatalf("submit on new leader, %v", err)
	}
	select {
	case r := <-ch:
		if r.Err != nil {
			t.Fatalf("commit on new leader, %v", r.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("majority side did not commit")
	}

	// heal. old leader should step down once it sees the higher term.
	for _, cn := range c.nodes {
		if cn.id == old.id {
			continue
		}
		c.net.partition(old.id, cn.id, false)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s, _ := old.node.State()
		if s != 2 /* Leader */ {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("old leader never stepped down after partition healed")
}
