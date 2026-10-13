package main

// sync.go — Bidirectional catch-up synchronisation for LANMesh.
//
// Two-phase protocol on /p2p-sync/1.0.0 (totally separate from the live
// /p2p-chat/1.0.0 flood relay):
//
//  Phase 1 – Digest handshake:
//    Initiator sends a SyncDigestRequest listing the highest timestamp it holds
//    for every group it knows about.  The responder compares and replies with
//    which groups it has newer data for.
//
//  Phase 2 – Chunked pull:
//    For each group that needs catch-up, the initiator sends SyncPullRequests
//    (50 messages per page max) until hasMore=false.  Each page is written to
//    BoltDB in a single transaction (one fsync) and then pushed to every local
//    WebSocket client via the hub so the UI updates in real-time.
//
// Resource protection:
//  • syncSem limits simultaneous sync streams to 2 (avoids CPU/Wi-Fi spikes).
//  • peerCooldown debounces rapid reconnects (45 s minimum between syncs).
//  • Synced messages are immediately markSeen() so they're never re-relayed
//    over the live chat protocol (no broadcast loop).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const syncProtocolID = "/p2p-sync/1.0.0"

// syncSem caps the number of simultaneous sync streams (inbound + outbound
// combined) so an ad-hoc LAN of budget laptops is never overwhelmed.
var syncSem = make(chan struct{}, 2)

// peerCooldown tracks when we last successfully synced with each peer so we
// can skip overly eager reconnect storms.
var (
	cooldownMu sync.Mutex
	cooldowns  = make(map[peer.ID]time.Time)
)

const cooldownDuration = 45 * time.Second

func canSyncWith(p peer.ID) bool {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	last, ok := cooldowns[p]
	return !ok || time.Since(last) >= cooldownDuration
}

func markSynced(p peer.ID) {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	cooldowns[p] = time.Now()
}

// ─── Wire message types ──────────────────────────────────────────────────────

// GroupWatermark is sent as part of a digest request.
type GroupWatermark struct {
	LatestTimestamp int64 `json:"latestTimestamp"`
}

// DigestRequest — initiator → responder.
type DigestRequest struct {
	Groups map[string]GroupWatermark `json:"groups"`
}

// DigestResponse — responder → initiator.
type DigestResponse struct {
	NeedsSync map[string]bool `json:"needsSync"` // groupID → true if responder has newer data
}

// PullRequest — initiator → responder (per group, per page).
type PullRequest struct {
	Group          string `json:"group"`
	SinceTimestamp int64  `json:"sinceTimestamp"`
	Limit          int    `json:"limit"`
}

// PullResponse — responder → initiator.
type PullResponse struct {
	Group     string        `json:"group"`
	Messages  []ChatMessage `json:"messages"`
	HasMore   bool          `json:"hasMore"`
	NextSince int64         `json:"nextSince,omitempty"`
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func sendJSON(w *bufio.Writer, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Flush()
}

func recvJSON(r *bufio.Reader, v interface{}) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, v)
}

// localDigest builds the initiator's view of its own high-watermarks.
func localDigest() map[string]GroupWatermark {
	groups := append(knownGroups(), "general")
	seen := make(map[string]struct{})
	out := make(map[string]GroupWatermark, len(groups))
	for _, g := range groups {
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		out[g] = GroupWatermark{LatestTimestamp: highWatermark(g)}
	}
	return out
}

// ─── Responder (incoming stream handler) ─────────────────────────────────────

// RegisterSyncHandler installs the /p2p-sync/1.0.0 stream handler on host.
// h is the WebSocket hub so back-filled messages can be pushed to open browser tabs.
func RegisterSyncHandler(host libp2phost.Host, h *hub) {
	host.SetStreamHandler(syncProtocolID, func(s network.Stream) {
		defer s.Close()

		// Throttle: if all semaphore slots are taken, politely close.
		select {
		case syncSem <- struct{}{}:
			defer func() { <-syncSem }()
		default:
			log.Println("[sync] too many concurrent streams; dropping inbound request")
			return
		}

		r := bufio.NewReader(s)
		w := bufio.NewWriter(s)

		// Phase 1 — receive digest request, send digest response.
		var req DigestRequest
		if err := recvJSON(r, &req); err != nil {
			log.Println("[sync] bad digest request:", err)
			return
		}

		// Compare initiator's watermarks to ours.
		resp := DigestResponse{NeedsSync: make(map[string]bool)}
		for group, wm := range req.Groups {
			our := highWatermark(group)
			if our > wm.LatestTimestamp {
				resp.NeedsSync[group] = true
			}
		}
		// Also include groups the initiator doesn't know about yet.
		for _, g := range knownGroups() {
			if _, known := req.Groups[g]; !known {
				if highWatermark(g) > 0 {
					resp.NeedsSync[g] = true
				}
			}
		}

		if err := sendJSON(w, resp); err != nil {
			log.Println("[sync] send digest response:", err)
			return
		}

		// Phase 2 — serve pull requests until the initiator is satisfied.
		for {
			var pull PullRequest
			if err := recvJSON(r, &pull); err != nil {
				// EOF means initiator is done — clean exit.
				return
			}

			msgs, hasMore := historyAfter(pull.Group, pull.SinceTimestamp, pull.Limit)
			var nextSince int64
			if len(msgs) > 0 {
				nextSince = msgs[len(msgs)-1].Timestamp
			}

			if err := sendJSON(w, PullResponse{
				Group:     pull.Group,
				Messages:  msgs,
				HasMore:   hasMore,
				NextSince: nextSince,
			}); err != nil {
				log.Println("[sync] send pull response:", err)
				return
			}
		}
	})
}

// ─── Initiator (outbound sync request) ───────────────────────────────────────

// InitiateSync opens a sync stream to peerID, runs the full digest + pull
// sequence, and back-fills the local DB + WebSocket hub with any missing messages.
// It is intentionally non-blocking: always called as `go InitiateSync(...)`.
func InitiateSync(ctx context.Context, host libp2phost.Host, peerID peer.ID, h *hub) {
	if !canSyncWith(peerID) {
		return
	}

	// Acquire a semaphore slot; drop if all taken.
	select {
	case syncSem <- struct{}{}:
		defer func() { <-syncSem }()
	default:
		return
	}

	markSynced(peerID)

	stream, err := host.NewStream(ctx, peerID, syncProtocolID)
	if err != nil {
		// Peer may not have the sync handler (older version or regular peer).
		return
	}
	defer stream.Close()

	r := bufio.NewReader(stream)
	w := bufio.NewWriter(stream)

	// Phase 1 — send our digest, receive response.
	digest := localDigest()
	if err := sendJSON(w, DigestRequest{Groups: digest}); err != nil {
		log.Println("[sync] send digest request:", err)
		return
	}

	var resp DigestResponse
	if err := recvJSON(r, &resp); err != nil {
		log.Println("[sync] recv digest response:", err)
		return
	}

	if len(resp.NeedsSync) == 0 {
		return // already up to date
	}

	// Notify local UI that a catch-up is in progress.
	notifySyncStatus(h, "syncing", fmt.Sprintf("Syncing %d group(s)…", len(resp.NeedsSync)))

	totalNew := 0

	// Phase 2 — pull pages for each group that has newer data.
	for group, needs := range resp.NeedsSync {
		if !needs {
			continue
		}
		since := highWatermark(group)

		for {
			if err := sendJSON(w, PullRequest{
				Group:          group,
				SinceTimestamp: since,
				Limit:          50,
			}); err != nil {
				log.Println("[sync] send pull request:", err)
				goto done
			}

			var page PullResponse
			if err := recvJSON(r, &page); err != nil {
				log.Println("[sync] recv pull response:", err)
				goto done
			}

			if len(page.Messages) > 0 {
				// Mark all as seen BEFORE saving so the live-relay loop
				// never re-broadcasts historical backfill messages.
				for _, m := range page.Messages {
					if m.MessageID != "" {
						markSeen(m.MessageID)
					}
				}

				if err := saveBatch(page.Messages); err != nil {
					log.Println("[sync] saveBatch:", err)
					goto done
				}

				totalNew += len(page.Messages)

				// Push every new message to open browser tabs so the
				// chat timeline updates smoothly during catch-up.
				for _, m := range page.Messages {
					h.broadcast(m)

					// Also feed to admin log if this is the admin node.
					if store := currentAdminStore(); store != nil {
						store.record(m)
					}
				}
			}

			if !page.HasMore {
				break
			}
			since = page.NextSince
		}
	}

done:
	if store := currentAdminStore(); store != nil {
		store.backfillFromDB()
	}
	if totalNew > 0 {
		notifySyncStatus(h, "done", fmt.Sprintf("Synced %d missed message(s)", totalNew))
	} else {
		notifySyncStatus(h, "done", "")
	}
}

// notifySyncStatus pushes a lightweight control message to all local WebSocket
// clients so the UI can show a "Syncing…" / "Up to date" indicator without
// polling.  status is "syncing" or "done"; detail is a human string.
func notifySyncStatus(h *hub, status, detail string) {
	h.broadcast(ChatMessage{
		Type:    "sync_status",
		Payload: status,
		// FileName carries the human-readable detail string without schema changes.
		FileName: detail,
	})
}
