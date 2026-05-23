package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
)

type strategy int

const (
	strategyRing strategy = iota
	strategyRendezvous
)

type hashRing struct {
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
// MD5 digest, first 8 bytes interpreted big-endian as uint64.  The truncation
// keeps the same distribution; the ring just lives in [0, 2^64) not [0, 2^128).
func ringHash(s string) uint64 {
	h := md5.Sum([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

func (r *hashRing) addNode(nodeID string) {
	for i := 0; i < r.virtualNodes; i++ {
		h := ringHash(fmt.Sprintf("%s:%d", nodeID, i))
		// bisect_right: insert after any equal elements (same as bisect.insort)
		pos := sort.Search(len(r.ring), func(j int) bool { return r.ring[j] > h })
		r.ring = append(r.ring, 0)
		copy(r.ring[pos+1:], r.ring[pos:])
		r.ring[pos] = h
		r.ringMap[h] = nodeID
	}
	r.nodes[nodeID] = struct{}{}
}

func (r *hashRing) nodeForKey(key string) string {
	if r.strat == strategyRendezvous {
		return r.rendezvousNode(key)
	}
	return r.getNode(key)
}

func (r *hashRing) getNode(key string) string {
	h := ringHash(key)
	// bisect_right: first index where ring[i] > h, then wrap
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

func (r *hashRing) virtualCount() int { return len(r.ring) }
