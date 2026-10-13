package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	bolt "go.etcd.io/bbolt"
)

const protocolID = "/p2p-chat/1.0.0"

//go:embed static/*
var staticFS embed.FS

// ---------------------------------------------------------------------
// Presence tracking. A lightweight "who's online" roster derived from
// heartbeat pings — each node broadcasts a type:"ping" every 20 s.
// Entries older than 60 s are considered offline and removed on the
// next read, so the roster self-heals without any explicit disconnect
// signal (which libp2p already handles separately via peer events).
// ---------------------------------------------------------------------

type presenceEntry struct {
	Nickname string `json:"nickname"`
	Seen     int64  `json:"seen"` // Unix millis
}

type presenceStore struct {
	mu      sync.Mutex
	entries map[string]presenceEntry // peerID -> entry
}

func newPresenceStore() *presenceStore {
	return &presenceStore{entries: make(map[string]presenceEntry)}
}

func (ps *presenceStore) update(peerID, nickname string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.entries[peerID] = presenceEntry{
		Nickname: nickname,
		Seen:     time.Now().UnixMilli(),
	}
}

// snapshot returns all entries seen within the last 60 seconds.
func (ps *presenceStore) snapshot() map[string]presenceEntry {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	cutoff := time.Now().Add(-60 * time.Second).UnixMilli()
	out := make(map[string]presenceEntry, len(ps.entries))
	for id, e := range ps.entries {
		if e.Seen >= cutoff {
			out[id] = e
		} else {
			delete(ps.entries, id)
		}
	}
	return out
}

var presence = newPresenceStore()

// ChatMessage represents every kind of thing that flows through the mesh.
// Type values:
//
//	"text"           - plain chat text
//	"audio"           - base64 voice note
//	"image"           - base64 image
//	"file"            - base64 arbitrary file, FileName holds the original name
//	"location"        - JSON {lat,lng,accuracy,timestamp} in Payload
//	"typing"          - ephemeral "X is typing" signal, never persisted
//	"reaction"        - JSON {targetId,emoji,action:"add"|"remove"} in Payload
//	"edit"            - JSON {targetId,newText} in Payload
//	"delete"          - Payload is just the targetId being deleted
//	"group_announce"  - system message, Payload is the new group's name
//	"whoami"          - backend -> its own UI only, never sent over libp2p
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
	FileSize  int64  `json:"fileSize,omitempty"` // byte count for file/image display
	ReplyTo   string `json:"replyTo,omitempty"`  // JSON-encoded {messageId,senderLabel,previewText}
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

// markSeen is defined in relay.go (package-level, delegates to globalSeen).

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
	// Each peer write runs in its own goroutine so a slow or unreachable
	// peer never stalls the relay for all other peers.
	for _, p := range host.Network().Peers() {
		if p == exclude {
			continue
		}
		p := p // capture loop variable
		go func() {
			stream, err := host.NewStream(ctx, p, protocolID)
			if err != nil {
				return
			}
			stream.Write(data)
			stream.Close()
		}()
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

const msgIndexBucket = "msg_index"

func initStore(path string) error {
	var err error
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(groupsBucket)); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(msgIndexBucket))
		return err
	})
}

// makeStorageKey builds the composite BoltDB key for a message:
// [8-byte BigEndian UnixMs timestamp][8-byte BigEndian hash of MessageID]
// This keeps messages time-ordered inside each bucket (BoltDB sorts keys
// lexicographically) and allows O(log N) cursor seeks for range queries.
func makeStorageKey(ts int64, msgID string) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[:8], uint64(ts))
	// Derive a stable 8-byte suffix from the MessageID so identical messages
	// get deterministic keys even when inserted by different peers.
	var h uint64
	for i, c := range []byte(msgID) {
		h ^= uint64(c) << (uint(i%8) * 8)
	}
	binary.BigEndian.PutUint64(key[8:], h)
	return key
}

// isKnownMessage returns true if the MessageID already exists in the global index.
// Used for O(1) duplicate detection during sync batch writes.
func isKnownMessage(tx *bolt.Tx, msgID string) bool {
	idx := tx.Bucket([]byte(msgIndexBucket))
	if idx == nil {
		return false
	}
	return idx.Get([]byte(msgID)) != nil
}

func saveGroup(name string) error {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" || name == "general" {
		return nil
	}
	return db.Update(func(tx *bolt.Tx) error {
		groups, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		if err != nil {
			return err
		}
		return groups.Put([]byte(name), []byte("1"))
	})
}

func saveMessage(msg ChatMessage) error {
	if msg.GroupID == "" {
		msg.GroupID = "general"
	}
	// Control and ephemeral messages must never be persisted as chat history
	if msg.Type == "group_announce" || msg.Type == "typing" || msg.Type == "ping" || msg.Type == "whoami" {
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
		// Idempotent write: skip if MessageID is already recorded.
		if msg.MessageID != "" && isKnownMessage(tx, msg.MessageID) {
			return nil
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
		if err != nil {
			return err
		}
		key := makeStorageKey(msg.Timestamp, msg.MessageID)
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, data); err != nil {
			return err
		}
		// Record in global index for fast future dedup checks.
		if msg.MessageID != "" {
			idx, err := tx.CreateBucketIfNotExists([]byte(msgIndexBucket))
			if err != nil {
				return err
			}
			return idx.Put([]byte(msg.MessageID), []byte(msg.GroupID))
		}
		return nil
	})
}

// saveBatch writes a slice of messages in a single BoltDB transaction —
// one fsync instead of N, which is critical for keeping Wi-Fi channels and
// local CPU from spiking during a catch-up sync.
func saveBatch(msgs []ChatMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	return db.Update(func(tx *bolt.Tx) error {
		idx, err := tx.CreateBucketIfNotExists([]byte(msgIndexBucket))
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if msg.GroupID == "" {
				msg.GroupID = "general"
			}
			if msg.Type == "group_announce" || msg.Type == "typing" || msg.Type == "ping" || msg.Type == "whoami" {
				continue
			}
			if msg.MessageID != "" && idx.Get([]byte(msg.MessageID)) != nil {
				continue // already have it
			}
			bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
			if err != nil {
				return err
			}
			key := makeStorageKey(msg.Timestamp, msg.MessageID)
			data, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			if err := bucket.Put(key, data); err != nil {
				return err
			}
			if msg.MessageID != "" {
				if err := idx.Put([]byte(msg.MessageID), []byte(msg.GroupID)); err != nil {
					return err
				}
			}
		}
		return nil
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
				// Filter out control and ghost messages from history replay
				if msg.Type == "ping" || msg.Type == "typing" || msg.Type == "group_announce" || msg.Type == "whoami" {
					return nil
				}
				if msg.Type == "text" && strings.TrimSpace(msg.Payload) == "" {
					return nil
				}
				out = append(out, msg)
			}
			return nil
		})
	})
	return out
}

// historyAfter returns up to `limit` messages in `groupID` with Timestamp > sinceMs,
// using a BoltDB cursor seek for O(log N) efficiency — no full scan needed.
// Results are in ascending timestamp order (oldest → newest), ready for a sync pull response.
func historyAfter(groupID string, sinceMs int64, limit int) (msgs []ChatMessage, hasMore bool) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		// Seek to first key whose timestamp is strictly greater than sinceMs.
		seek := makeStorageKey(sinceMs+1, "")
		for k, v := c.Seek(seek); k != nil; k, v = c.Next() {
			if len(msgs) >= limit {
				hasMore = true
				break
			}
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err != nil {
				continue
			}
			if msg.Type == "ping" || msg.Type == "typing" || msg.Type == "group_announce" || msg.Type == "whoami" {
				continue
			}
			msgs = append(msgs, msg)
		}
		return nil
	})
	return
}

// highWatermark returns the highest Timestamp stored for a group, or 0 if empty.
func highWatermark(groupID string) int64 {
	var hw int64
	db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		if k, _ := c.Last(); k != nil && len(k) >= 8 {
			hw = int64(binary.BigEndian.Uint64(k[:8]))
		}
		return nil
	})
	return hw
}

// historyPage returns up to `limit` messages for `groupID`, starting from
// `offset` from the END (i.e. offset=0 returns the newest `limit` messages).
func historyPage(groupID string, limit, offset int) (msgs []ChatMessage, total int) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		var valid []ChatMessage
		b.ForEach(func(k, v []byte) error { //nolint:errcheck
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err == nil {
				if msg.Type == "ping" || msg.Type == "typing" || msg.Type == "group_announce" || msg.Type == "whoami" {
					return nil
				}
				if msg.Type == "text" && strings.TrimSpace(msg.Payload) == "" {
					return nil
				}
				valid = append(valid, msg)
			}
			return nil
		})
		total = len(valid)
		start := total - offset - limit
		if start < 0 {
			start = 0
		}
		end := total - offset
		if end <= 0 {
			return nil
		}
		if start < end {
			msgs = valid[start:end]
		}
		return nil
	})
	return
}

// historyRecent returns the most recent `limit` messages for `groupID` in ascending chronological order.
func historyRecent(groupID string, limit int) []ChatMessage {
	msgs, _ := historyPage(groupID, limit, 0)
	return msgs
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

// loadOrCreatePeerKey loads a persistent Ed25519 identity key from path,
// or generates and saves a new one. Stable peer IDs survive restarts —
// critical for the sync protocol and admin discovery.
func loadOrCreatePeerKey(path string) (libp2pcrypto.PrivKey, error) {
	if data, err := os.ReadFile(path); err == nil {
		if key, err := libp2pcrypto.UnmarshalPrivateKey(data); err == nil {
			return key, nil
		}
	}
	key, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate peer key: %w", err)
	}
	data, err := libp2pcrypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal peer key: %w", err)
	}
	return key, os.WriteFile(path, data, 0600)
}

func openBrowser(url string) {
	var err error
	switch runtime.GOOS {
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default: // linux, bsd, etc.
		err = exec.Command("xdg-open", url).Start()
	}
	if err != nil {
		log.Printf("could not open browser automatically: %v (visit %s manually)", err, url)
	}
}

func main() {
	// Admin binaries (compiled with -ldflags="-X main.adminSecret=...") default to
	// port 9090 and a separate data-dir so they can coexist on the same machine
	// alongside a regular peer node on 8080 without any DB/key conflicts.
	defaultPort := 8080
	defaultDataDir := "."
	if adminSecret != "" {
		defaultPort = 9090
		defaultDataDir = "./admin-data"
	}

	port := flag.Int("port", defaultPort, "HTTP/WebSocket port to listen on")
	dataDir := flag.String("data-dir", defaultDataDir, "directory for this node's bbolt DB and admin log")
	noBrowser := flag.Bool("no-browser", false, "disable automatic browser opening")
	cliAdminSecret := flag.String("admin-secret", "", "enable admin capabilities with this password (alternative to build-time -ldflags)")
	cliNickname := flag.String("nickname", "", "display nickname for this node (defaults to 'Admin' for admin nodes)")
	p2pPort := flag.Int("p2p-port", 4001, "libp2p TCP transport listen port")
	flag.Parse()

	if *cliAdminSecret != "" {
		adminSecret = *cliAdminSecret
	} else if envSecret := os.Getenv("LANMESH_ADMIN_SECRET"); envSecret != "" && adminSecret == "" {
		adminSecret = envSecret
	}

	if *cliNickname != "" {
		setConfiguredNickname(*cliNickname)
	} else if adminSecret != "" {
		setConfiguredNickname("Admin")
	}

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

	if adminSecret != "" {
		if store, err := enableAdminLogging(); err == nil && store != nil {
			store.backfillFromDB()
		}
	}

	// Load or create a persistent Ed25519 identity so peer IDs survive restarts.
	keyPath := filepath.Join(*dataDir, "peer.key")
	peerKey, err := loadOrCreatePeerKey(keyPath)
	if err != nil {
		log.Fatal("peer key:", err)
	}

	host, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *p2pPort)),
		libp2p.Security(tls.ID, tls.New),
		libp2p.Identity(peerKey),
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

		// Update presence roster for every message that carries a sender+nickname.
		if msg.Sender != "" && msg.Nickname != "" {
			presence.update(msg.Sender, msg.Nickname)
		}

		if msg.Type == "group_announce" && msg.Payload != "" {
			saveGroup(msg.Payload)
		}

		if msg.Type != "ping" && msg.Type != "typing" && msg.Type != "whoami" {
			stats.record(msg.Hops)
		}

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
		log.Printf("mDNS discovery warning: %v\n", err)
	} else {
		fmt.Println("mDNS discovery started")
	}

	// Secondary zero-config discovery: UDP broadcast beacon for Android and cross-platform LAN
	startDiscoveryBeacon(ctx, host)

	// Register sync handler so peers can pull our history.
	RegisterSyncHandler(host, h)

	// Trigger an outbound sync whenever a new peer is directly connected.
	// We hook into libp2p's network notifier rather than mDNS alone, so
	// this fires for any connection event (mDNS, manual dial, etc.).
	host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			go InitiateSync(ctx, host, c.RemotePeer(), h)
			go func() {
				// Immediately announce our presence to the newly connected peer
				// so their online roster and nickname update in sub-second time.
				time.Sleep(150 * time.Millisecond)
				nick := getConfiguredNickname()
				if nick == "" {
					nick = host.ID().String()[:min(12, len(host.ID().String()))]
				}
				ping := ChatMessage{
					Type:      "ping",
					Sender:    host.ID().String(),
					Nickname:  nick,
					MessageID: newMessageID(),
					Timestamp: time.Now().UnixMilli(),
				}
				markSeen(ping.MessageID)
				relayToPeers(ctx, host, "", ping)
			}()
		},
	})

	mux := http.NewServeMux()
	registerAdminRoutes(mux, func(name string) {
		// Admin created a new group: persist → broadcast to local WS clients → relay over mesh.
		name = strings.TrimSpace(strings.ToLower(name))
		saveGroup(name)
		msg := ChatMessage{
			Type:      "group_announce",
			Payload:   name,
			GroupID:   name,
			MessageID: newMessageID(),
			Timestamp: time.Now().UnixMilli(),
			Sender:    host.ID().String(),
		}
		markSeen(msg.MessageID)
		h.broadcast(msg)
		relayToPeers(ctx, host, "", msg)
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	})
	// Register WebRTC signaling: /lanmesh-call/1.0.0 stream + /signal WebSocket.
	RegisterCallHandler(ctx, host, mux)

	// Serve static web UI: first check local disk (for dev iteration),
	// otherwise serve the embedded HTML bundle inside the executable (standalone deployment).

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatal("failed to load embedded static filesystem:", err)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat("./static/index.html"); err == nil {
			http.FileServer(http.Dir("./static")).ServeHTTP(w, r)
			return
		}
		http.FileServer(http.FS(staticSub)).ServeHTTP(w, r)
	})

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

	// /groups returns the list of group names this node knows about.
	// Used by the frontend's search and group-selector features.
	mux.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"groups": knownGroups(),
		})
	})

	// /history?group=general&limit=50&offset=0
	// Returns a paginated slice of the stored history for one group,
	// newest-first window. offset counts from the END of the log so that
	// repeated "load earlier" calls walk backwards through history.
	// Response: { total: N, messages: [...] }
	mux.HandleFunc("/history", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		group := r.URL.Query().Get("group")
		if group == "" {
			group = "general"
		}
		limit, offset := 50, 0
		if v := r.URL.Query().Get("limit"); v != "" {
			fmt.Sscanf(v, "%d", &limit)
		}
		if v := r.URL.Query().Get("offset"); v != "" {
			fmt.Sscanf(v, "%d", &offset)
		}
		msgs, total := historyPage(group, limit, offset)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total":    total,
			"messages": msgs,
		})
	})

	// /presence returns the live roster of nodes seen in the last 60 s.
	// It also synthesises any directly-connected libp2p peers that haven't
	// sent a presence ping yet, so the peer count is never under-reported
	// just because a node joined before its 20-second heartbeat fired.
	// Each entry has nickname and when they were last seen (Unix ms).
	// Response: { self, roster: { peerID: { nickname, seen } } }
	mux.HandleFunc("/presence", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		roster := presence.snapshot()
		nowMs := time.Now().UnixMilli()
		if selfNick := getConfiguredNickname(); selfNick != "" {
			roster[host.ID().String()] = presenceEntry{
				Nickname: selfNick,
				Seen:     nowMs,
			}
		}
		// Inject any directly connected libp2p peers not yet in the roster.
		for _, p := range host.Network().Peers() {
			pid := p.String()
			if _, ok := roster[pid]; !ok {
				roster[pid] = presenceEntry{
					Nickname: pid[:min(12, len(pid))],
					Seen:     nowMs,
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":   host.ID().String(),
			"roster": roster,
		})
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()
		conn.SetReadLimit(64 * 1024 * 1024) // 16MB limit to support large base64 attachments and photos

		h.add(conn)
		defer h.remove(conn)

		// Tell this tab its own peer ID, so it can tell its own messages
		// apart from others' once they come back through the broadcast.
		conn.WriteJSON(ChatMessage{
			Type:     "whoami",
			Payload:  host.ID().String(),
			Nickname: getConfiguredNickname(),
		})

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
		groupsToReplay := append([]string{"general"}, knownGroups()...)
		for _, g := range groupsToReplay {
			for _, m := range historyRecent(g, 50) {
				conn.WriteJSON(m)
			}
		}

		// Loop to read messages FROM this UI and fan them out everywhere
		for {
			var msg ChatMessage
			if err := conn.ReadJSON(&msg); err != nil {
				break
			}

			// Group creation is admin-only via POST /admin/groups.
			// Drop any group_announce sent directly from a browser tab.
			if msg.Type == "group_announce" {
				continue
			}

			msg.Sender = host.ID().String()
			if msg.GroupID == "" {
				msg.GroupID = "general"
			}
			msg.MessageID = newMessageID()
			msg.Hops = 0
			msg.Timestamp = time.Now().UnixMilli()
			markSeen(msg.MessageID) // so we don't re-relay it if it ever loops back to us

			// Track our own presence (the frontend sets nickname on every msg)
			if msg.Nickname != "" {
				setConfiguredNickname(msg.Nickname)
				presence.update(msg.Sender, msg.Nickname)
			}

			if msg.Type != "ping" && msg.Type != "typing" && msg.Type != "whoami" {
				stats.record(msg.Hops)
			}

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
	baseURL := fmt.Sprintf("http://localhost%s", addr)
	// Default screen is always the chat screen at /.
	// Admin nodes can access the dashboard whenever needed via the "Admin ⚡" header pill.
	launchURL := baseURL
	fmt.Printf("Server running at %s\n", baseURL)
	if adminSecret != "" {
		fmt.Printf("Admin capabilities active. Access dashboard at: %s/admin\n", baseURL)
	}

	if !*noBrowser {
		go func() {
			time.Sleep(300 * time.Millisecond) // Give the HTTP listener a split-second to bind
			openBrowser(launchURL)
		}()
	}

	// Periodic self-ping: broadcast a presence heartbeat every 20 s so
	// every node on the mesh can maintain an up-to-date online roster.
	// We mark it seen before relaying so it won't bounce back to us.
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// We don't know the current nickname on the backend; the frontend
			// sends pings with its own nickname. This server-side ping uses
			// the peer ID as a fallback so remote nodes at least know we exist.
			pingNick := host.ID().String()[:12]
			if nick := getConfiguredNickname(); nick != "" {
				pingNick = nick
			}
			ping := ChatMessage{
				Type:      "ping",
				Sender:    host.ID().String(),
				Nickname:  pingNick,
				MessageID: newMessageID(),
				Timestamp: time.Now().UnixMilli(),
			}
			markSeen(ping.MessageID)
			relayToPeers(ctx, host, "", ping)
		}
	}()

	log.Fatal(http.ListenAndServe(addr, mux))
}
