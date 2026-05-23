package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type strategy int

const (
	strategyRing strategy = iota
	strategyRendezvous
)

type hashRing struct {
	mu           sync.RWMutex
	ring         []uint64
	ringMap      map[uint64]string
	nodes        map[string]struct{}
	virtualNodes int
	strat        strategy
}

func newHashRing(virtualNodes int, strat strategy) *hashRing {
	return &hashRing{
		ring:         make([]uint64, 0),
		ringMap:      make(map[uint64]string),
		nodes:        make(map[string]struct{}),
		virtualNodes: virtualNodes,
		strat:        strat,
	}
}

// ringHash mirrors Python's int(hashlib.md5(s.encode()).hexdigest(), 16) — same
// MD5 digest, first 8 bytes interpreted big-endian as uint64.
func ringHash(s string) uint64 {
	h := md5.Sum([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

func (r *hashRing) addNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.virtualNodes; i++ {
		h := ringHash(fmt.Sprintf("%s:%d", nodeID, i))
		pos := sort.Search(len(r.ring), func(j int) bool { return r.ring[j] > h })
		r.ring = append(r.ring, 0)
		copy(r.ring[pos+1:], r.ring[pos:])
		r.ring[pos] = h
		r.ringMap[h] = nodeID
	}
	r.nodes[nodeID] = struct{}{}
}

func (r *hashRing) removeNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.virtualNodes; i++ {
		h := ringHash(fmt.Sprintf("%s:%d", nodeID, i))
		// bisect_left: find exact position of h
		pos := sort.Search(len(r.ring), func(j int) bool { return r.ring[j] >= h })
		if pos < len(r.ring) && r.ring[pos] == h {
			r.ring = append(r.ring[:pos], r.ring[pos+1:]...)
		}
		delete(r.ringMap, h)
	}
	delete(r.nodes, nodeID)
}

// nodeForKey returns (nodeID, true) or ("", false) if the ring is empty.
func (r *hashRing) nodeForKey(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return "", false
	}
	if r.strat == strategyRendezvous {
		return r.rendezvousNode(key), true
	}
	return r.getNode(key), true
}

func (r *hashRing) getNode(key string) string {
	h := ringHash(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] > h })
	idx = idx % len(r.ring)
	return r.ringMap[r.ring[idx]]
}

func (r *hashRing) rendezvousNode(key string) string {
	var best string
	var bestScore uint64
	for nodeID := range r.nodes {
		score := ringHash(fmt.Sprintf("%s:%s", nodeID, key))
		if best == "" || score > bestScore {
			best = nodeID
			bestScore = score
		}
	}
	return best
}

func (r *hashRing) virtualCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.ring)
}

func (r *hashRing) liveNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	return out
}
