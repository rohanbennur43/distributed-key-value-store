package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"dkv/internal/server"
)

func main() {
	id := flag.Int("id", 1, "Server ID (unique positive integer)")
	addr := flag.String("addr", ":5001", "Listen address (host:port)")
	peersFlag := flag.String("peers", "", "Comma-separated peer list: id=host:port,id=host:port")
	dataDir := flag.String("data", "", "Data directory (default: data/node-<id>)")
	flag.Parse()

	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node-%d", *id)
	}

	peers := make(map[int32]string)
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			p = strings.TrimSpace(p)
			parts := strings.SplitN(p, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid peer format %q, expected id=host:port", p)
			}
			var pid int
			if _, err := fmt.Sscanf(parts[0], "%d", &pid); err != nil {
				log.Fatalf("invalid peer ID %q: %v", parts[0], err)
			}
			peers[int32(pid)] = parts[1]
		}
	}

	cfg := server.NodeConfig{
		ID:      int32(*id),
		Addr:    *addr,
		DataDir: *dataDir,
		Peers:   peers,
	}

	node, err := server.NewNode(cfg)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("failed to start node: %v", err)
	}

	log.Printf("Node %d running on %s", *id, *addr)

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("Shutting down node %d...", *id)
	node.Stop()
}
