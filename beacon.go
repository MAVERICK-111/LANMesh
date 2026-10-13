package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const beaconPort = 9999

type BeaconPayload struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

// getBroadcastAddresses enumerates local network interfaces and returns
// both interface-specific directed broadcast addresses (e.g. 192.168.1.255)
// and the global 255.255.255.255 broadcast address.
func getBroadcastAddresses(port int) []*net.UDPAddr {
	var addrs []*net.UDPAddr

	// Global broadcast fallback
	if global, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("255.255.255.255:%d", port)); err == nil {
		addrs = append(addrs, global)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		// On Android without netlink permissions, net.Interfaces() may fail; global address is returned
		return addrs
	}

	for _, iface := range ifaces {
		// Skip down and loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrsList, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrsList {
			ipnet, ok := addr.(*net.IPNet)
			if !ok || ipnet.IP == nil {
				continue
			}

			ip4 := ipnet.IP.To4()
			if ip4 == nil || ip4.IsLoopback() {
				continue
			}

			// Compute subnet broadcast: IP | (^Mask)
			mask := ipnet.Mask
			if len(mask) == 4 {
				bcastIP := net.IPv4(
					ip4[0]|^mask[0],
					ip4[1]|^mask[1],
					ip4[2]|^mask[2],
					ip4[3]|^mask[3],
				)
				if bcastAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", bcastIP.String(), port)); err == nil {
					addrs = append(addrs, bcastAddr)
				}
			}
		}
	}

	return addrs
}

// startDiscoveryBeacon runs a lightweight LAN UDP broadcast service on port 9999.
// This guarantees instant peer discovery across Android, Windows, Mac, and Linux
// even when Android restricts netlink routing tables or multicast DNS.
func startDiscoveryBeacon(ctx context.Context, h host.Host) {
	// 1. Listen for peer UDP broadcast announcements
	laddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("0.0.0.0:%d", beaconPort))
	if err != nil {
		log.Printf("beacon resolve error: %v\n", err)
		return
	}
	recvConn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		log.Printf("beacon listen error on port %d: %v\n", beaconPort, err)
		return
	}

	go func() {
		defer recvConn.Close()
		buf := make([]byte, 2048)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			_ = recvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, raddr, err := recvConn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			var payload BeaconPayload
			if err := json.Unmarshal(buf[:n], &payload); err != nil {
				continue
			}

			// Ignore beacons sent by ourselves
			if payload.PeerID == h.ID().String() {
				continue
			}

			remotePID, err := peer.Decode(payload.PeerID)
			if err != nil {
				continue
			}

			// If already connected, skip
			if len(h.Network().ConnsToPeer(remotePID)) > 0 {
				continue
			}

			var dialAddrs []ma.Multiaddr
			senderIP := ""
			if raddr != nil && raddr.IP != nil {
				senderIP = raddr.IP.String()
			}

			for _, addrStr := range payload.Addrs {
				// If the address contains 0.0.0.0 (common on Android without netlink permissions),
				// replace 0.0.0.0 with the sender's actual observed UDP IP
				if senderIP != "" && strings.Contains(addrStr, "/ip4/0.0.0.0/") {
					addrStr = strings.Replace(addrStr, "/ip4/0.0.0.0/", fmt.Sprintf("/ip4/%s/", senderIP), 1)
				}
				if m, err := ma.NewMultiaddr(addrStr); err == nil {
					dialAddrs = append(dialAddrs, m)
				}
			}

			// Essential Android Fix: If Android failed to resolve its local IP via netlink,
			// always prepend the exact senderIP observed by our UDP socket on port 4001
			if senderIP != "" {
				if directAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/4001", senderIP)); err == nil {
					dialAddrs = append([]ma.Multiaddr{directAddr}, dialAddrs...)
				}
			}

			if len(dialAddrs) > 0 {
				info := peer.AddrInfo{
					ID:    remotePID,
					Addrs: dialAddrs,
				}
				go func(pi peer.AddrInfo) {
					dialCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
					defer cancel()
					if err := h.Connect(dialCtx, pi); err == nil {
						fmt.Printf("Beacon auto-connected to mesh peer: %s\n", pi.ID)
					}
				}(info)
			}
		}
	}()

	// 2. Broadcast our presence to subnet broadcast and 255.255.255.255 every 2 seconds
	go func() {
		// Use a local UDP socket to send broadcast datagrams
		srcAddr, _ := net.ResolveUDPAddr("udp4", "0.0.0.0:0")
		sendConn, err := net.ListenUDP("udp4", srcAddr)
		if err != nil {
			log.Printf("beacon sender error: %v\n", err)
			return
		}
		defer sendConn.Close()

		ticker := time.NewTicker(2000 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				addrs := h.Addrs()
				var addrStrings []string
				for _, a := range addrs {
					addrStrings = append(addrStrings, a.String())
				}
				payload := BeaconPayload{
					PeerID: h.ID().String(),
					Addrs:  addrStrings,
				}
				data, err := json.Marshal(payload)
				if err != nil {
					continue
				}

				targets := getBroadcastAddresses(beaconPort)
				for _, target := range targets {
					_, _ = sendConn.WriteToUDP(data, target)
				}
			}
		}
	}()
	fmt.Printf("UDP broadcast beacon running on port %d\n", beaconPort)
}
