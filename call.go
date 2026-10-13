package main

// call.go — WebRTC signaling over libp2p
//
// Architecture:
//   - Browser UI connects to /signal WebSocket (per-node, local).
//   - UI sends JSON SignalEnvelope with To = target peerID.
//   - This backend opens a /lanmesh-call/1.0.0 stream to the target peer
//     and writes the envelope JSON.
//   - The receiving backend's stream handler decodes the envelope and
//     delivers it to any /signal WebSocket clients attached to that node.
//
// Message types:
//   call-offer   — SDP offer
//   call-answer  — SDP answer
//   call-ice     — ICE candidate
//   call-end     — hang up / leave room
//   call-reject  — callee rejected
//   room-join    — peer joins a group room
//   room-leave   — peer leaves a group room

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const callProtocolID = "/lanmesh-call/1.0.0"

// SignalEnvelope is the unit of exchange for the call/signaling layer.
type SignalEnvelope struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	To      string          `json:"to"`
	RoomID  string          `json:"roomId,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// signalHub keeps the set of /signal WebSocket connections local to this node.
type signalHub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func newSignalHub() *signalHub {
	return &signalHub{clients: make(map[*websocket.Conn]bool)}
}

func (sh *signalHub) add(c *websocket.Conn) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.clients[c] = true
}

func (sh *signalHub) remove(c *websocket.Conn) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	delete(sh.clients, c)
}

func (sh *signalHub) deliver(env SignalEnvelope) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for c := range sh.clients {
		if err := c.WriteJSON(env); err != nil {
			c.Close()
			delete(sh.clients, c)
		}
	}
}

// RegisterCallHandler wires up the libp2p stream handler and /signal WS endpoint.
func RegisterCallHandler(ctx context.Context, host libp2phost.Host, mux *http.ServeMux) {
	sh := newSignalHub()

	// ── libp2p stream handler ─────────────────────────────────────────────────
	host.SetStreamHandler(callProtocolID, func(s network.Stream) {
		defer s.Close()
		data, err := io.ReadAll(s)
		if err != nil {
			return
		}
		var env SignalEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			log.Println("[call] bad envelope:", err)
			return
		}
		sh.deliver(env)
	})

	// ── /signal WebSocket ─────────────────────────────────────────────────────
	mux.HandleFunc("/signal", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("[call] /signal upgrade:", err)
			return
		}
		defer conn.Close()
		sh.add(conn)
		defer sh.remove(conn)

		for {
			var env SignalEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			env.From = host.ID().String()

			if env.To == "" {
				// Broadcast (room-join / room-leave).
				callForwardToAll(ctx, host, env)
				sh.deliver(env)
				continue
			}

			targetID, err := peer.Decode(env.To)
			if err != nil {
				log.Println("[call] bad target:", env.To, err)
				continue
			}

			data, _ := json.Marshal(env)
			go func() {
				stream, err := host.NewStream(ctx, targetID, callProtocolID)
				if err != nil {
					log.Println("[call] stream open failed:", env.To, err)
					errEnv := SignalEnvelope{Type: "call-end", From: env.To, To: host.ID().String()}
					errEnv.Payload, _ = json.Marshal(map[string]string{"reason": "unreachable"})
					sh.deliver(errEnv)
					return
				}
				defer stream.Close()
				stream.Write(data)
			}()
		}
	})
}

// callForwardToAll sends an envelope to every connected peer (for room broadcasts).
func callForwardToAll(ctx context.Context, host libp2phost.Host, env SignalEnvelope) {
	data, _ := json.Marshal(env)
	for _, p := range host.Network().Peers() {
		p := p
		go func() {
			s, err := host.NewStream(ctx, p, callProtocolID)
			if err != nil {
				return
			}
			defer s.Close()
			s.Write(data)
		}()
	}
}
