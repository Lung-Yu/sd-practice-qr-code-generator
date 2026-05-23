package main

import "testing"

func TestNodesForKey_ReturnsTwoDistinctNodes(t *testing.T) {
	r := newHashRing(50, strategyRing)
	r.addNode("node1"); r.addNode("node2"); r.addNode("node3")
	nodes := r.nodesForKey("key4", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Fatalf("expected distinct nodes, got %q and %q", nodes[0], nodes[1])
	}
}

func TestNodesForKey_PrimaryMatchesNodeForKey(t *testing.T) {
	r := newHashRing(50, strategyRing)
	r.addNode("node1"); r.addNode("node2"); r.addNode("node3")
	for _, key := range []string{"foo", "bar", "baz", "key1", "key10"} {
		single, _ := r.nodeForKey(key)
		multi := r.nodesForKey(key, 2)
		if multi[0] != single {
			t.Errorf("key %q: nodesForKey[0]=%q, nodeForKey=%q", key, multi[0], single)
		}
	}
}

func TestNodesForKey_OneNodeInRing(t *testing.T) {
	r := newHashRing(50, strategyRing)
	r.addNode("node1")
	nodes := r.nodesForKey("anykey", 2)
	if len(nodes) != 1 || nodes[0] != "node1" {
		t.Fatalf("expected [node1], got %v", nodes)
	}
}

func TestNodesForKey_EmptyRing(t *testing.T) {
	r := newHashRing(50, strategyRing)
	if nodes := r.nodesForKey("anykey", 2); nodes != nil {
		t.Fatalf("expected nil for empty ring, got %v", nodes)
	}
}

func TestNodesForKey_RendezvousPrimaryMatchesNodeForKey(t *testing.T) {
	r := newHashRing(50, strategyRendezvous)
	r.addNode("node1"); r.addNode("node2"); r.addNode("node3")
	for _, key := range []string{"foo", "bar", "baz"} {
		single, _ := r.nodeForKey(key)
		multi := r.nodesForKey(key, 2)
		if len(multi) != 2 {
			t.Errorf("key %q: expected 2 nodes, got %d", key, len(multi))
			continue
		}
		if multi[0] != single {
			t.Errorf("key %q: nodesForKey[0]=%q, nodeForKey=%q", key, multi[0], single)
		}
	}
}
