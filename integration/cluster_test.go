package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"dkv/internal/server"
	pb "dkv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testCluster manages a 3-node cluster for integration tests.
type testCluster struct {
	nodes []*server.Node
	addrs []string
	dirs  []string
}

func newCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	basePort := 16000 + (os.Getpid() % 1000) * 10

	addrs := make([]string, size)
	for i := 0; i < size; i++ {
		addrs[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	dirs := make([]string, size)
	nodes := make([]*server.Node, size)

	for i := 0; i < size; i++ {
		dir := t.TempDir()
		dirs[i] = dir

		peers := make(map[int32]string)
		for j := 0; j < size; j++ {
			if i != j {
				peers[int32(j+1)] = addrs[j]
			}
		}

		cfg := server.NodeConfig{
			ID:      int32(i + 1),
			Addr:    addrs[i],
			DataDir: dir,
			Peers:   peers,
		}

		node, err := server.NewNode(cfg)
		if err != nil {
			t.Fatalf("create node %d: %v", i+1, err)
		}
		nodes[i] = node
	}

	// Start all nodes.
	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node %d: %v", i+1, err)
		}
	}

	// Wait for gRPC servers to be ready.
	time.Sleep(500 * time.Millisecond)

	return &testCluster{nodes: nodes, addrs: addrs, dirs: dirs}
}

func (c *testCluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func (c *testCluster) kvClient(t *testing.T, nodeIdx int) pb.KVServiceClient {
	t.Helper()
	conn, err := grpc.Dial(c.addrs[nodeIdx],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("dial node %d: %v", nodeIdx+1, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewKVServiceClient(conn)
}

func (c *testCluster) adminClient(t *testing.T, nodeIdx int) pb.AdminServiceClient {
	t.Helper()
	conn, err := grpc.Dial(c.addrs[nodeIdx],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("dial admin node %d: %v", nodeIdx+1, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewAdminServiceClient(conn)
}

// --- Tests ---

func TestLinearizableWriteAndRead(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	kv1 := c.kvClient(t, 0)
	kv2 := c.kvClient(t, 1)
	kv3 := c.kvClient(t, 2)

	// Write via node 1.
	resp, err := kv1.Put(context.Background(), &pb.PutRequest{
		Key: "x", Value: "42", ClientId: "test-client", OpId: 1,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("put error: %s", resp.Error)
	}

	// Read via node 2 — must see value (linearizable).
	gr2, err := kv2.Get(context.Background(), &pb.GetRequest{
		Key: "x", ClientId: "reader-2", OpId: 1,
	})
	if err != nil {
		t.Fatalf("get node2: %v", err)
	}
	if !gr2.Found || gr2.Value != "42" {
		t.Fatalf("node2: expected x=42, got found=%v value=%s", gr2.Found, gr2.Value)
	}

	// Read via node 3.
	gr3, err := kv3.Get(context.Background(), &pb.GetRequest{
		Key: "x", ClientId: "reader-3", OpId: 1,
	})
	if err != nil {
		t.Fatalf("get node3: %v", err)
	}
	if !gr3.Found || gr3.Value != "42" {
		t.Fatalf("node3: expected x=42, got found=%v value=%s", gr3.Found, gr3.Value)
	}
}

func TestDuplicateSuppression(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	kv1 := c.kvClient(t, 0)
	admin1 := c.adminClient(t, 0)

	// Get baseline metrics.
	mBefore, _ := admin1.Metrics(context.Background(), &pb.MetricsRequest{})
	suppBefore := mBefore.Counters["duplicate_suppressed_total"]

	clientID := "dedup-test"
	opID := int64(100)

	// First request: should succeed.
	resp, err := kv1.Put(context.Background(), &pb.PutRequest{
		Key: "dup-key", Value: "dup-val", ClientId: clientID, OpId: opID,
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("first put: err=%v resp=%v", err, resp)
	}

	// Send 49 more duplicates.
	for i := 0; i < 49; i++ {
		resp, err = kv1.Put(context.Background(), &pb.PutRequest{
			Key: "dup-key", Value: "dup-val", ClientId: clientID, OpId: opID,
		})
		if err != nil || resp.Error != "" {
			t.Fatalf("duplicate put %d: err=%v resp=%v", i, err, resp)
		}
	}

	mAfter, _ := admin1.Metrics(context.Background(), &pb.MetricsRequest{})
	suppAfter := mAfter.Counters["duplicate_suppressed_total"]
	newSuppressed := suppAfter - suppBefore

	t.Logf("Duplicates suppressed: %d / 49", newSuppressed)
	if newSuppressed < 45 {
		t.Fatalf("expected ~49 duplicates suppressed, got %d", newSuppressed)
	}

	// Verify value is correct (single write).
	gr, _ := kv1.Get(context.Background(), &pb.GetRequest{
		Key: "dup-key", ClientId: "dedup-reader", OpId: 1,
	})
	if gr.Value != "dup-val" {
		t.Fatalf("expected dup-val, got %s", gr.Value)
	}
}

func TestAppendAndDelete(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	kv := c.kvClient(t, 0)

	// Put + Append.
	kv.Put(context.Background(), &pb.PutRequest{
		Key: "msg", Value: "hello", ClientId: "t", OpId: 1,
	})
	kv.Append(context.Background(), &pb.AppendRequest{
		Key: "msg", Value: " world", ClientId: "t", OpId: 2,
	})

	gr, _ := kv.Get(context.Background(), &pb.GetRequest{
		Key: "msg", ClientId: "r", OpId: 1,
	})
	if gr.Value != "hello world" {
		t.Fatalf("expected 'hello world', got %q", gr.Value)
	}

	// Delete.
	dr, _ := kv.Delete(context.Background(), &pb.DeleteRequest{
		Key: "msg", ClientId: "t", OpId: 3,
	})
	if !dr.Found {
		t.Fatal("delete should report found")
	}

	gr2, _ := kv.Get(context.Background(), &pb.GetRequest{
		Key: "msg", ClientId: "r", OpId: 2,
	})
	if gr2.Found {
		t.Fatal("key should be deleted")
	}
}

func TestConcurrentWrites(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	var wg sync.WaitGroup
	errCh := make(chan error, 30)

	// 3 clients writing 10 ops each to different nodes.
	for clientNum := 0; clientNum < 3; clientNum++ {
		wg.Add(1)
		go func(cn int) {
			defer wg.Done()
			kv := c.kvClient(t, cn)
			clientID := fmt.Sprintf("concurrent-%d", cn)
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("c%d-k%d", cn, i)
				resp, err := kv.Put(context.Background(), &pb.PutRequest{
					Key: key, Value: fmt.Sprintf("v%d", i), ClientId: clientID, OpId: int64(i + 1),
				})
				if err != nil {
					errCh <- fmt.Errorf("client %d op %d: %v", cn, i, err)
				} else if resp.Error != "" {
					errCh <- fmt.Errorf("client %d op %d: %s", cn, i, resp.Error)
				}
			}
		}(clientNum)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify all keys readable from all nodes.
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		kv := c.kvClient(t, nodeIdx)
		for cn := 0; cn < 3; cn++ {
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("c%d-k%d", cn, i)
				gr, err := kv.Get(context.Background(), &pb.GetRequest{
					Key: key, ClientId: fmt.Sprintf("verify-%d-%d", nodeIdx, cn),
					OpId: int64(100 + cn*10 + i),
				})
				if err != nil {
					t.Errorf("get %s from node %d: %v", key, nodeIdx+1, err)
					continue
				}
				expected := fmt.Sprintf("v%d", i)
				if gr.Value != expected {
					t.Errorf("node %d: %s=%s, expected %s", nodeIdx+1, key, gr.Value, expected)
				}
			}
		}
	}
}

func TestHealthAndMetrics(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	for i := 0; i < 3; i++ {
		admin := c.adminClient(t, i)
		h, err := admin.Health(context.Background(), &pb.HealthRequest{})
		if err != nil {
			t.Fatalf("health node %d: %v", i+1, err)
		}
		if !h.Healthy {
			t.Fatalf("node %d not healthy", i+1)
		}
		if h.ServerId != int32(i+1) {
			t.Fatalf("expected server ID %d, got %d", i+1, h.ServerId)
		}
	}

	// Do a write, then check metrics.
	kv := c.kvClient(t, 0)
	kv.Put(context.Background(), &pb.PutRequest{
		Key: "m", Value: "1", ClientId: "metrics-test", OpId: 1,
	})

	admin := c.adminClient(t, 0)
	m, _ := admin.Metrics(context.Background(), &pb.MetricsRequest{})
	if m.Counters["applied_ops_total"] < 1 {
		t.Fatal("expected at least 1 applied op")
	}
}

func TestFetchLog(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	kv := c.kvClient(t, 0)
	for i := 1; i <= 5; i++ {
		kv.Put(context.Background(), &pb.PutRequest{
			Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i),
			ClientId: "log-test", OpId: int64(i),
		})
	}

	admin := c.adminClient(t, 0)
	logResp, err := admin.FetchLog(context.Background(), &pb.FetchLogRequest{
		FromIndex: 1, ToIndex: 0,
	})
	if err != nil {
		t.Fatalf("fetch log: %v", err)
	}
	if len(logResp.Entries) < 5 {
		t.Fatalf("expected >= 5 log entries, got %d", len(logResp.Entries))
	}

	chosenCount := 0
	for _, e := range logResp.Entries {
		if e.Chosen {
			chosenCount++
		}
	}
	t.Logf("Log entries: %d total, %d chosen", len(logResp.Entries), chosenCount)
}
