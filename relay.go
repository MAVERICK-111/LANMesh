package main

import "sync"

// maxSeenEntries caps memory use for long-running nodes. Once exceeded,
// the oldest entries are evicted — fine for dedup purposes since a
// message that old has long since finished propagating.
const maxSeenEntries = 5000

// seenSet tracks message IDs this node has already processed, so the
// flood-relay logic in main.go doesn't re-broadcast (or re-display) the
// same message forever as it bounces around a mesh with cycles.
type seenSet struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string
}

func newSeenSet() *seenSet {
	return &seenSet{seen: make(map[string]struct{})}
}

// markSeen returns true the first time it's called for a given id — the
// caller should process/relay the message. It returns false on every
// subsequent call for that same id — the caller should silently drop it.
func (s *seenSet) markSeen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[id]; ok {
		return false
	}
	s.seen[id] = struct{}{}
	s.order = append(s.order, id)
	if len(s.order) > maxSeenEntries {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.seen, oldest)
	}
	return true
}