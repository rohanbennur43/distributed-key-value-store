package server

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"dkv/internal/metrics"
	"dkv/internal/paxos"
	"dkv/internal/storage"
	"dkv/internal/store"
	pb "dkv/proto/kvpb"

	"google.golang.org/grpc"
)

// NodeConfig holds startup configuration for a replica.
type NodeConfig struct {
	ID       int32
	Addr     string            // listen address, e.g. ":5001"
	DataDir  string            // persistence directory
	Peers    map[int32]string  // peerID -> "host:port"
}

// Node is a single replica in the distributed KV store.
type Node struct {
	id      int32
	addr    string
	engine  *paxos.Engine
	store   *store.KVStore
	persist storage.Store
	metrics *metrics.Metrics
	peers   []*GRPCPeer

	grpcServer *grpc.Server

	// Apply loop.
	applyMu    sync.Mutex
	applyCh    chan int64
	stopCh     chan struct{}
	stopped    bool
}

// NewNode creates and initializes a replica.
func NewNode(cfg NodeConfig) (*Node, error) {
	totalNodes := len(cfg.Peers) + 1

	// Open persistent storage.
	persist, err := storage.NewBoltStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}

	m := &metrics.Metrics{}
	kvStore := store.NewKVStore(persist)

	n := &Node{
		id:      cfg.ID,
		addr:    cfg.Addr,
		persist: persist,
		store:   kvStore,
		metrics: m,
		applyCh: make(chan int64, 1024),
		stopCh:  make(chan struct{}),
	}

	// Create engine with callback.
	onChosen := func(index int64, op *paxos.Operation) {
		m.ChosenIndex.Store(index)
		select {
		case n.applyCh <- index:
		default:
		}
	}
	n.engine = paxos.NewEngine(cfg.ID, totalNodes, persist, onChosen)

	// Restore state from disk.
	if err := n.restore(); err != nil {
		persist.Close()
		return nil, fmt.Errorf("restore: %w", err)
	}

	// Create peer clients.
	peers := make([]*GRPCPeer, 0, len(cfg.Peers))
	paxosPeers := make([]paxos.PeerClient, 0, len(cfg.Peers))
	for pid, paddr := range cfg.Peers {
		p := NewGRPCPeer(pid, paddr)
		peers = append(peers, p)
		paxosPeers = append(paxosPeers, p)
	}
	n.peers = peers
	n.engine.SetPeers(paxosPeers)

	return n, nil
}

// restore loads persisted state and replays the chosen log.
func (n *Node) restore() error {
	// Load chosen entries.
	chosen, err := n.persist.LoadAllChosen()
	if err != nil {
		return fmt.Errorf("load chosen: %w", err)
	}
	n.engine.RestoreChosen(chosen)

	// Apply chosen entries in order.
	maxIdx := n.engine.MaxChosenIndex()
	for i := int64(1); i <= maxIdx; i++ {
		op := n.engine.GetChosenValue(i)
		if op == nil {
			continue
		}
		_, dup := n.store.Apply(i, op)
		if !dup {
			n.metrics.AppliedOpsTotal.Add(1)
		}
	}
	n.metrics.LastAppliedIndex.Store(n.store.LastAppliedIndex())

	log.Printf("[node %d] restored: maxChosen=%d lastApplied=%d",
		n.id, maxIdx, n.store.LastAppliedIndex())
	return nil
}

// Start begins serving gRPC and the background apply loop.
func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", n.addr, err)
	}

	n.grpcServer = grpc.NewServer()
	pb.RegisterKVServiceServer(n.grpcServer, &KVServiceServer{node: n})
	pb.RegisterPaxosServiceServer(n.grpcServer, &PaxosServiceServer{node: n})
	pb.RegisterAdminServiceServer(n.grpcServer, &AdminServiceServer{node: n})

	// Start apply loop.
	go n.applyLoop()

	// Start catch-up routine.
	go n.catchUpLoop()

	log.Printf("[node %d] listening on %s (peers: %d)", n.id, n.addr, len(n.peers))
	go func() {
		if err := n.grpcServer.Serve(lis); err != nil {
			log.Printf("[node %d] grpc serve error: %v", n.id, err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	n.stopped = true
	close(n.stopCh)
	n.engine.Shutdown()
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}
	for _, p := range n.peers {
		p.Close()
	}
	n.persist.Close()
	log.Printf("[node %d] stopped", n.id)
}

// applyLoop continuously applies chosen log entries in order.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
			n.applyUpTo(n.engine.MaxChosenIndex())
		}
	}
}

// applyUpTo applies all chosen entries up to the given index.
func (n *Node) applyUpTo(targetIndex int64) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	lastApplied := n.store.LastAppliedIndex()
	for i := lastApplied + 1; i <= targetIndex; i++ {
		op := n.engine.GetChosenValue(i)
		if op == nil {
			// Gap in the log — can't proceed past this.
			break
		}
		_, dup := n.store.Apply(i, op)
		if dup {
			n.metrics.DuplicateSuppressedTotal.Add(1)
		} else {
			n.metrics.AppliedOpsTotal.Add(1)
		}
		n.metrics.LastAppliedIndex.Store(i)
	}
}

// catchUpLoop periodically fetches chosen entries from peers.
func (n *Node) catchUpLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.doCatchUp()
		}
	}
}

// doCatchUp fetches missing chosen entries from peers.
func (n *Node) doCatchUp() {
	fromIndex := n.store.LastAppliedIndex() + 1

	for _, peer := range n.peers {
		entries, maxChosen, err := peer.FetchChosen(fromIndex)
		if err != nil {
			continue
		}

		for _, e := range entries {
			n.engine.HandleLearn(e.Index, &e.Value)
			n.metrics.CatchupEntriesApplied.Add(1)
		}

		if maxChosen > 0 {
			n.applyUpTo(maxChosen)
		}
		break // One successful peer is enough.
	}
}

// GetStore returns the KV store (for testing).
func (n *Node) GetStore() *store.KVStore {
	return n.store
}

// GetEngine returns the Paxos engine (for testing).
func (n *Node) GetEngine() *paxos.Engine {
	return n.engine
}

// GetMetrics returns the metrics (for testing).
func (n *Node) GetMetrics() *metrics.Metrics {
	return n.metrics
}
