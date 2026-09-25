package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	bolt "go.etcd.io/bbolt"
)

const protocolID = "/p2p-chat/1.0.0"

// ChatMessage represents every kind of thing that flows through the mesh.
// Type values:
//   "text"           - plain chat text
//   "audio"           - base64 voice note
//   "image"           - base64 image
//   "file"            - base64 arbitrary file, FileName holds the original name
//   "location"        - JSON {lat,lng,accuracy,timestamp} in Payload
//   "typing"          - ephemeral "X is typing" signal, never persisted
//   "reaction"        - JSON {targetId,emoji,action:"add"|"remove"} in Payload
//   "edit"            - JSON {targetId,newText} in Payload
//   "delete"          - Payload is just the targetId being deleted
//   "group_announce"  - system message, Payload is the new group's name
//   "whoami"          - backend -> its own UI only, never sent over libp2p
//
// Reactions/edits/deletes are NOT rendered as their own chat bubbles — the
// frontend treats them as annotations on an existing message, looked up by
// the targetId embedded in their Payload. This means the backend needs zero
// special-casing for them: they flow through the exact same persist/
// broadcast/relay pipeline as everything else.
type ChatMessage struct {
	Type      string `json:"type"`
	Payload   string `json:"payload"`
	Sender    string `json:"sender"`
	GroupID   string `json:"groupId"`
	MessageID string `json:"messageId"`
	Hops      int    `json:"hops"`
	Timestamp int64  `json:"timestamp"`
	Nickname  string `json:"nickname"`
	FileName  string `json:"fileName,omitempty"` // set for type "file"
}

const maxHops = 6 // generous ceiling so a chain like mech -> library -> boys is nowhere close to it

func newMessageID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// ---------------------------------------------------------------------
// Multi-hop relay. Range is not the same as mDNS reachability: a node in
// the mech building and a node in the boys hostel may never directly
// discover each other, but a node in the library — in range of both —
// can. Every node floods a NEW message on to every peer it's directly
// connected to (other than whoever it just came from); the seen-set
// below is what stops that flood from looping forever once the network
// has more than a straight line (e.g. any node with 2+ neighbors).
// ---------------------------------------------------------------------

var (
	seenMu sync.Mutex
	seen   = make(map[string]bool)
)

func markSeen(id string) bool {
	seenMu.Lock()
	defer seenMu.Unlock()
	if seen[id] {
		return true // already seen -> caller should drop/not relay it again
	}
	seen[id] = true
	if len(seen) > 5000 { // crude cap so a long-running node doesn't leak memory
		seen = make(map[string]bool)
	}
	return false
}

// relayToPeers forwards msg to every currently connected peer except
// `exclude` (the peer we just received it from, if any). Called both for
// locally-originated messages (exclude = "") and for messages arriving
// from another peer that still have hops left.
func relayToPeers(ctx context.Context, host libp2phost.Host, exclude peer.ID, msg ChatMessage) {
	if msg.Hops >= maxHops {
		return
	}
	msg.Hops++
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, p := range host.Network().Peers() {
		if p == exclude {
			continue
		}
		stream, err := host.NewStream(ctx, p, protocolID)
		if err != nil {
			continue
		}
		stream.Write(data)
		stream.Close()
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ---------------------------------------------------------------------
// Persistent storage (bbolt: embedded, pure Go, no cgo — cross-compiles
// cleanly to ARM later for a Raspberry Pi). One bucket per group holding
// ordered chat history, plus one index bucket tracking which groups this
// node has ever seen, so a freshly connected/refreshed tab can be caught
// up on both "what groups exist" and "what was said in them" instead of
// starting from nothing every time.
// ---------------------------------------------------------------------

var db *bolt.DB

const groupsBucket = "__groups__"

func initStore(path string) error {
	var err error
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		return err
	})
}

func saveMessage(msg ChatMessage) error {
	if msg.GroupID == "" {
		msg.GroupID = "general"
	}
	return db.Update(func(tx *bolt.Tx) error {
		groups, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		if err != nil {
			return err
		}
		if err := groups.Put([]byte(msg.GroupID), []byte("1")); err != nil {
			return err
		}

		// group_announce is not itself chat history, just an index update.
		// typing is ephemeral by design and must never be replayed on
		// reconnect, so it's excluded from persistence the same way.
		if msg.Type == "group_announce" || msg.Type == "typing" {
			return nil
		}

		bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
		if err != nil {
			return err
		}
		seq, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, seq)
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
}

func knownGroups() []string {
	var groups []string
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			groups = append(groups, string(k))
			return nil
		})
	})
	return groups
}

func historyFor(groupID string) []ChatMessage {
	var out []ChatMessage
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err == nil {
				out = append(out, msg)
			}
			return nil
		})
	})
	return out
}

// ---------------------------------------------------------------------
// Broadcast hub. Multiple browser tabs (and later, multiple paired
// mobile clients) can be attached to this one backend at once. The old
// code used a single shared `chan ChatMessage` with one consumer
// goroutine per connection — since a Go channel hands each item to
// exactly ONE receiver, two attached tabs would just split messages
// between them instead of both seeing everything. This hub instead
// keeps an explicit set of live connections and writes every message to
// every one of them.
// ---------------------------------------------------------------------

type hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func newHub() *hub {
	return &hub{clients: make(map[*websocket.Conn]bool)}
}

func (h *hub) add(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
}

func (h *hub) remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

func (h *hub) broadcast(msg ChatMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			c.Close()
			delete(h.clients, c)
		}
	}
}

// ---------------------------------------------------------------------
// Network health tracking. This is deliberately lightweight and purely
// in-memory (not persisted) — it's a live snapshot of "how alive does
// the mesh look from this node's point of view" for the /health
// endpoint, not a historical record. connectedPeers is read fresh from
// libp2p each time (it's already authoritative there), but messagesSeen
// / maxHopsSeen / lastActivity need to be tracked as messages pass
// through, since libp2p itself doesn't remember that for us.
// ---------------------------------------------------------------------

type healthStats struct {
	mu           sync.Mutex
	messagesSeen int
	maxHopsSeen  int
	lastActivity time.Time
}

var stats = &healthStats{}

func (s *healthStats) record(hops int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messagesSeen++
	if hops > s.maxHopsSeen {
		s.maxHopsSeen = hops
	}
	s.lastActivity = time.Now()
}

func (s *healthStats) snapshot() (messagesSeen int, maxHops int, lastActivity time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messagesSeen, s.maxHopsSeen, s.lastActivity
}

func main() {
	port := flag.Int("port", 8080, "HTTP/WebSocket port to listen on")
	dataDir := flag.String("data-dir", ".", "directory for this node's bbolt DB and admin log")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatal("failed to create data dir:", err)
	}
	setAdminDataDir(*dataDir)

	ctx := context.Background()

	dbPath := filepath.Join(*dataDir, "lanmesh.db")
	if err := initStore(dbPath); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Force TLS 1.3 as the security transport for every peer connection.
	host, err := libp2p.New(
		libp2p.Security(tls.ID, tls.New),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	fmt.Println("Your Peer ID:", host.ID())

	h := newHub()

	// Handle incoming p2p streams from other Go backends on the mesh
	host.SetStreamHandler(protocolID, func(s network.Stream) {
		defer s.Close()
		remote := s.Conn().RemotePeer()

		var msg ChatMessage
		data, err := io.ReadAll(s)
		if err != nil {
			return
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}

		// Drop anything we've already relayed once — without this, a
		// network with any loop in it (e.g. three buildings that are all
		// pairwise in range) would flood the same message forever.
		if msg.MessageID == "" || markSeen(msg.MessageID) {
			return
		}

		stats.record(msg.Hops)

		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist incoming message:", err)
		}
		h.broadcast(msg)

		// Multi-hop relay: pass it on to every OTHER peer we're directly
		// connected to (not back to whoever just sent it to us). This is
		// what lets a message cross from a building that can't reach the
		// final destination directly, via one that can reach both.
		relayToPeers(ctx, host, remote, msg)

		// Log every message this node ever sees, if admin logging is on.
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	})

	if err := setupMDNS(ctx, host); err != nil {
		log.Fatal(err)
	}
	fmt.Println("mDNS discovery started")

	mux := http.NewServeMux()
	registerAdminRoutes(mux)
	mux.Handle("/", http.FileServer(http.Dir("./static")))

	// Peer/mesh topology: this node's own view of who it's directly
	// connected to right now. Note this is inherently local — each node
	// only knows its own direct connections, not the full mesh graph, so
	// the frontend's topology view is "what does this one node see",
	// not a global map. Combined with a message's Hops field (see
	// /health and the reaction/edit annotations), it's still enough to
	// tell direct-connection traffic apart from genuinely relayed traffic.
	mux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		peers := host.Network().Peers()
		ids := make([]string, 0, len(peers))
		for _, p := range peers {
			ids = append(ids, p.String())
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":  host.ID().String(),
			"peers": ids,
		})
	})

	// Network health: a rough, best-effort signal of mesh liveness from
	// this node's vantage point. secondsSinceLastActivity is -1 until
	// this node has seen its first message.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		messagesSeen, maxHops, lastActivity := stats.snapshot()
		lastActivitySecs := -1.0
		if !lastActivity.IsZero() {
			lastActivitySecs = time.Since(lastActivity).Seconds()
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connectedPeers":           len(host.Network().Peers()),
			"messagesSeen":             messagesSeen,
			"maxHopsObserved":          maxHops,
			"secondsSinceLastActivity": lastActivitySecs,
		})
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()

		h.add(conn)
		defer h.remove(conn)

		// Tell this tab its own peer ID, so it can tell its own messages
		// apart from others' once they come back through the broadcast.
		conn.WriteJSON(ChatMessage{Type: "whoami", Payload: host.ID().String()})

		// Catch this tab up: announce every known group, then replay each
		// group's stored history, in order. Reactions/edits/deletes are
		// interleaved in with regular messages here since they're stored
		// the same way — the frontend applies them as annotations during
		// replay in the same order they originally happened.
		for _, g := range knownGroups() {
			if g == "general" {
				continue // already present in the UI by default
			}
			conn.WriteJSON(ChatMessage{Type: "group_announce", Payload: g, GroupID: g})
		}
		for _, g := range knownGroups() {
			for _, m := range historyFor(g) {
				conn.WriteJSON(m)
			}
		}

		// Loop to read messages FROM this UI and fan them out everywhere
		for {
			var msg ChatMessage
			if err := conn.ReadJSON(&msg); err != nil {
				break
			}

			msg.Sender = host.ID().String()
			if msg.GroupID == "" {
				msg.GroupID = "general"
			}
			msg.MessageID = newMessageID()
			msg.Hops = 0
			msg.Timestamp = time.Now().UnixMilli()
			markSeen(msg.MessageID) // so we don't re-relay it if it ever loops back to us

			stats.record(msg.Hops)

			if err := saveMessage(msg); err != nil {
				log.Println("failed to persist outgoing message:", err)
			}

			// Every locally attached client (other tabs, future mobile
			// clients) sees it immediately, including the sender's own
			// tab — the frontend distinguishes "own" messages by peer ID,
			// not by an optimistic local echo, so multiple tabs stay in
			// sync with each other and with history replay.
			h.broadcast(msg)

			// Send to every directly connected peer; each of THEM will, in
			// turn, relay it on to their own peers (see the stream handler
			// above) — this is what makes it multi-hop rather than
			// single-hop full-mesh.
			relayToPeers(ctx, host, "", msg)

			// Log locally-originated messages too, if admin logging is on.
			if store := currentAdminStore(); store != nil {
				store.record(msg)
			}
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("Server running at http://localhost%s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}