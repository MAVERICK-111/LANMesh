package main

import "sync"

// maxSeenEntries caps memory use for long-running nodes. Once exceeded,
// the oldest entries are evicted via an ordered queue — fine for dedup
// purposes since a message that old has long since finished propagating.
const maxSeenEntries = 5000

// seenSet tracks message IDs this node has already processed, so the
// flood-relay logic in main.go doesn't re-broadcast (or re-display) the
// same message forever as it bounces around a mesh with cycles.
//
// The global singleton `globalSeen` is the authoritative dedup store;
// main.go calls markSeen() which delegates here.
type seenSet struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string // FIFO queue for eviction
}

func newSeenSet() *seenSet {
	return &seenSet{seen: make(map[string]struct{})}
}

// add returns true if the id is NEW (caller should process/relay it),
// false if it was already seen (caller should silently drop it).
func (s *seenSet) add(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[id]; ok {
		return false // already seen → drop
	}
	s.seen[id] = struct{}{}
	s.order = append(s.order, id)
	if len(s.order) > maxSeenEntries {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.seen, oldest)
	}
	return true // novel → process
}

// globalSeen is the node-wide dedup singleton used by main.go.
var globalSeen = newSeenSet()

// markSeen is the package-level helper called by main.go.
// Returns true if this id has already been seen (caller should drop it),
// false if it is novel (caller should process and relay it).
// NOTE: semantics match the original main.go usage: true = duplicate.
func markSeen(id string) bool {
	return !globalSeen.add(id)
}