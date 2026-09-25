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

type ChatMessage struct {
	Type      string `json:"type"`
	Payload   string `json:"payload"`
	Sender    string `json:"sender"`
	GroupID   string `json:"groupId"`
	MessageID string `json:"messageId"`
	Hops      int    `json:"hops"`
	Timestamp int64  `json:"timestamp"`
	Nickname  string `json:"nickname"`
}

const maxHops = 6

func newMessageID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

var (
	seenMu sync.Mutex
	seen   = make(map[string]bool)
)

func markSeen(id string) bool {
	seenMu.Lock()
	defer seenMu.Unlock()
	if seen[id] {
		return true
	}
	seen[id] = true
	if len(seen) > 5000 {
		seen = make(map[string]bool)
	}
	return false
}

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
		if msg.Type == "group_announce" {
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

func main() {
	port := flag.Int("port", 8080, "HTTP/WebSocket port to listen on")
    dataDir := flag.String("data-dir", ".", "directory for this node's bbolt DB and admin log")
    fmt.Printf("DEBUG: os.Args = %#v\n", os.Args)
    flag.Parse()
    fmt.Printf("DEBUG: port=%d dataDir=%q\n", *port, *dataDir)

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

	host, err := libp2p.New(
		libp2p.Security(tls.ID, tls.New),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	fmt.Println("Your Peer ID:", host.ID())

	h := newHub()

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

		if msg.MessageID == "" || markSeen(msg.MessageID) {
			return
		}

		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist incoming message:", err)
		}
		h.broadcast(msg)

		relayToPeers(ctx, host, remote, msg)

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

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()

		h.add(conn)
		defer h.remove(conn)

		conn.WriteJSON(ChatMessage{Type: "whoami", Payload: host.ID().String()})

		for _, g := range knownGroups() {
			if g == "general" {
				continue
			}
			conn.WriteJSON(ChatMessage{Type: "group_announce", Payload: g, GroupID: g})
		}
		for _, g := range knownGroups() {
			for _, m := range historyFor(g) {
				conn.WriteJSON(m)
			}
		}

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
			markSeen(msg.MessageID)

			if err := saveMessage(msg); err != nil {
				log.Println("failed to persist outgoing message:", err)
			}

			h.broadcast(msg)
			relayToPeers(ctx, host, "", msg)

			if store := currentAdminStore(); store != nil {
				store.record(msg)
			}
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("Server running at http://localhost%s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}