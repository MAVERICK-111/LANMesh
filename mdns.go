package main

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const mdnsServiceName = "p2p-voice-mdns"

type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Println("Found peer:", pi.ID)
	err := n.h.Connect(context.Background(), pi)
	if err != nil {
		fmt.Println("Connection failed:", err)
	}
}

func setupMDNS(ctx context.Context, h host.Host) error {
	notifee := &discoveryNotifee{h: h}
	service := mdns.NewMdnsService(h, mdnsServiceName, notifee)
	return service.Start()
}