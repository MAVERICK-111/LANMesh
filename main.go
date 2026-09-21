package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
)

const protocolID = "/p2p-chat/1.0.0"

// ChatMessage represents text, voice, image, and location messages.
// Type identifies the payload kind, and Payload carries the actual data.
type ChatMessage struct {
	ID        string `json:"id"`        // unique per message; used to dedup flood-relay hops
	Type      string `json:"type"`      // "text" | "audio" | "image" | "location"
	Payload   string `json:"payload"`   // text content, base64 audio/image, or JSON location string
	Sender    string `json:"sender"`    // Peer ID
	Nickname  string `json:"nickname"`  // human-friendly display name chosen by the sender
	GroupID   string `json:"groupId"`   // Group this message belongs to (e.g. "general", "team-a")
	Timestamp int64  `json:"timestamp"` // unix millis, set once at the originating node
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Global channel to pass messages from libp2p to the WebSocket UI
var uiMessages = make(chan ChatMessage, 32)

func genID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// broadcastToPeers fans a message out to every currently-connected peer
// except `exclude` (typically whoever just sent it to us, so we don't
// immediately bounce it straight back).
func broadcastToPeers(ctx context.Context, h lp2phost.Host, exclude peer.ID, msg ChatMessage) {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, pid := range h.Network().Peers() {
		if pid == exclude {
			continue
		}
		stream, err := h.NewStream(ctx, pid, protocolID)
		if err != nil {
			continue
		}
		stream.Write(msgBytes)
		stream.Close()
	}
}

func main() {
	// -admin is now just a convenience for non-interactive startup (e.g.
	// scripted deployments later). The normal path is the in-app prompt:
	// the frontend calls POST /admin/enable at runtime, so nobody needs
	// to know this flag exists.
	adminMode := flag.Bool("admin", false, "start with admin logging already enabled")
	dataDir := flag.String("data-dir", ".", "directory for the admin log file")
	httpPort := flag.String("port", "8080", "HTTP/WebSocket server port")
	flag.Parse()

	setAdminDataDir(*dataDir)
	if *adminMode {
		if _, err := enableAdminLogging(); err != nil {
			log.Fatal("failed to open admin log: ", err)
		}
	}

	ctx := context.Background()

	// Force TLS 1.3 as the security transport for every peer connection.
	host, err := libp2p.New(
		libp2p.Security(tls.ID, tls.New),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	fmt.Println("Your Peer ID:", host.ID())

	seen := newSeenSet()

	// Handle incoming p2p streams
	host.SetStreamHandler(protocolID, func(s network.Stream) {
		defer s.Close()
		var msg ChatMessage

		data, err := io.ReadAll(s)
		if err != nil {
			return
		}
		if err := json.Unmarshal(data, &msg); err != nil || msg.ID == "" {
			return
		}

		// Dedup: if we've already handled this message ID (we originated
		// it, or relayed it once already via a different neighbor), drop
		// it here so the mesh doesn't flood forever.
		if !seen.markSeen(msg.ID) {
			return
		}

		uiMessages <- msg
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}

		// Multi-hop relay: forward to every OTHER connected peer besides
		// whoever just sent it to us. This is what lets a message cross
		// regions that aren't in direct range of each other — e.g. a
		// device in the mech building relays through a device in the
		// library to reach the boys hostel, as long as each hop is in
		// range of the next.
		remote := s.Conn().RemotePeer()
		go broadcastToPeers(ctx, host, remote, msg)
	})

	// Start mDNS (using your existing setupMDNS function)
	err = setupMDNS(ctx, host)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("mDNS discovery started")

	// Setup HTTP Server for UI and WebSocket
	http.Handle("/", http.FileServer(http.Dir("./static")))
	registerAdminRoutes(http.DefaultServeMux)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()

		// Goroutine to send messages FROM libp2p TO the UI
		go func() {
			for msg := range uiMessages {
				conn.WriteJSON(msg)
			}
		}()

		// Loop to read messages FROM the UI and send TO libp2p peers
		for {
			var msg ChatMessage
			err := conn.ReadJSON(&msg)
			if err != nil {
				break
			}

			msg.Sender = host.ID().String()
			if msg.GroupID == "" {
				msg.GroupID = "general"
			}
			msg.ID = genID()
			msg.Timestamp = time.Now().UnixMilli()

			seen.markSeen(msg.ID) // record our own message so we ignore it if it loops back
			if store := currentAdminStore(); store != nil {
				store.record(msg)
			}

			// exclude nothing — a locally-originated message goes to
			// every directly-connected peer, then relays onward from there.
			broadcastToPeers(ctx, host, "", msg)
		}
	})

	fmt.Printf("Server running at http://localhost:%s\n", *httpPort)
	log.Fatal(http.ListenAndServe(":"+*httpPort, nil))
}