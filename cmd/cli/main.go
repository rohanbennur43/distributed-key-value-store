package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "dkv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "put":
		doPut(os.Args[2:])
	case "get":
		doGet(os.Args[2:])
	case "append":
		doAppend(os.Args[2:])
	case "delete":
		doDelete(os.Args[2:])
	case "health":
		doHealth(os.Args[2:])
	case "metrics":
		doMetrics(os.Args[2:])
	case "fetch-log":
		doFetchLog(os.Args[2:])
	case "drop-rules":
		doDropRules(os.Args[2:])
	case "scenario":
		if len(os.Args) < 3 {
			fmt.Println("Usage: dkv-cli scenario <basic|duplicates|failover|recovery|partition|loadtest>")
			os.Exit(1)
		}
		runScenario(os.Args[2])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`dkv-cli — Distributed KV Store Client

Commands:
  put       --addr host:port --key K --value V [--client-id CID --op-id OID]
  get       --addr host:port --key K
  append    --addr host:port --key K --value V [--client-id CID --op-id OID]
  delete    --addr host:port --key K [--client-id CID --op-id OID]
  health    --addr host:port
  metrics   --addr host:port
  fetch-log --addr host:port [--from N --to N]
  drop-rules --addr host:port --rules "targetID:rate,..."
  scenario  <basic|duplicates|failover|recovery|partition|loadtest>`)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
}

// --- KV commands ---

func doPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	key := fs.String("key", "", "")
	value := fs.String("value", "", "")
	clientID := fs.String("client-id", "cli", "")
	opID := fs.Int64("op-id", 0, "")
	fs.Parse(args)

	if *opID == 0 {
		*opID = time.Now().UnixNano()
	}

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewKVServiceClient(conn).Put(context.Background(), &pb.PutRequest{
		Key: *key, Value: *value, ClientId: *clientID, OpId: *opID,
	})
	if err != nil {
		log.Fatalf("put: %v", err)
	}
	if resp.Error != "" {
		fmt.Printf("ERROR: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("OK put %s=%s\n", *key, *value)
}

func doGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	key := fs.String("key", "", "")
	clientID := fs.String("client-id", "cli-reader", "")
	opID := fs.Int64("op-id", 0, "")
	fs.Parse(args)

	if *opID == 0 {
		*opID = time.Now().UnixNano()
	}

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewKVServiceClient(conn).Get(context.Background(), &pb.GetRequest{
		Key: *key, ClientId: *clientID, OpId: *opID,
	})
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	if resp.Error != "" {
		fmt.Printf("ERROR: %s\n", resp.Error)
		os.Exit(1)
	}
	if resp.Found {
		fmt.Printf("%s=%s\n", *key, resp.Value)
	} else {
		fmt.Printf("%s not found\n", *key)
	}
}

func doAppend(args []string) {
	fs := flag.NewFlagSet("append", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	key := fs.String("key", "", "")
	value := fs.String("value", "", "")
	clientID := fs.String("client-id", "cli", "")
	opID := fs.Int64("op-id", 0, "")
	fs.Parse(args)

	if *opID == 0 {
		*opID = time.Now().UnixNano()
	}

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewKVServiceClient(conn).Append(context.Background(), &pb.AppendRequest{
		Key: *key, Value: *value, ClientId: *clientID, OpId: *opID,
	})
	if err != nil {
		log.Fatalf("append: %v", err)
	}
	if resp.Error != "" {
		fmt.Printf("ERROR: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("OK append %s+=%s\n", *key, *value)
}

func doDelete(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	key := fs.String("key", "", "")
	clientID := fs.String("client-id", "cli", "")
	opID := fs.Int64("op-id", 0, "")
	fs.Parse(args)

	if *opID == 0 {
		*opID = time.Now().UnixNano()
	}

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewKVServiceClient(conn).Delete(context.Background(), &pb.DeleteRequest{
		Key: *key, ClientId: *clientID, OpId: *opID,
	})
	if err != nil {
		log.Fatalf("delete: %v", err)
	}
	if resp.Error != "" {
		fmt.Printf("ERROR: %s\n", resp.Error)
		os.Exit(1)
	}
	if resp.Found {
		fmt.Printf("OK deleted %s (existed)\n", *key)
	} else {
		fmt.Printf("OK deleted %s (did not exist)\n", *key)
	}
}

// --- Admin commands ---

func doHealth(args []string) {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	fs.Parse(args)

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewAdminServiceClient(conn).Health(context.Background(), &pb.HealthRequest{})
	if err != nil {
		log.Fatalf("health: %v", err)
	}
	fmt.Printf("Node %d: healthy=%v chosenIndex=%d lastApplied=%d\n",
		resp.ServerId, resp.Healthy, resp.ChosenIndex, resp.LastAppliedIndex)
}

func doMetrics(args []string) {
	fs := flag.NewFlagSet("metrics", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	fs.Parse(args)

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewAdminServiceClient(conn).Metrics(context.Background(), &pb.MetricsRequest{})
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	for k, v := range resp.Counters {
		fmt.Printf("  %-35s %d\n", k, v)
	}
}

func doFetchLog(args []string) {
	fs := flag.NewFlagSet("fetch-log", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	from := fs.Int64("from", 1, "")
	to := fs.Int64("to", 0, "")
	fs.Parse(args)

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewAdminServiceClient(conn).FetchLog(context.Background(), &pb.FetchLogRequest{
		FromIndex: *from, ToIndex: *to,
	})
	if err != nil {
		log.Fatalf("fetch-log: %v", err)
	}
	for _, e := range resp.Entries {
		if e.Chosen {
			fmt.Printf("  [%d] %s %s=%s (client=%s, op=%d)\n",
				e.Index, e.Value.Type, e.Value.Key, e.Value.Value, e.Value.ClientId, e.Value.OpId)
		} else {
			fmt.Printf("  [%d] (unchosen)\n", e.Index)
		}
	}
}

func doDropRules(args []string) {
	fs := flag.NewFlagSet("drop-rules", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "")
	rules := fs.String("rules", "", "targetID:rate,... (e.g. 2:1.0,3:0.5) or 'clear'")
	fs.Parse(args)

	conn, err := dial(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	var pbRules []*pb.DropRule
	if *rules != "clear" && *rules != "" {
		for _, r := range strings.Split(*rules, ",") {
			r = strings.TrimSpace(r)
			var tid int32
			var rate float64
			if _, err := fmt.Sscanf(r, "%d:%f", &tid, &rate); err != nil {
				log.Fatalf("invalid rule %q: %v", r, err)
			}
			pbRules = append(pbRules, &pb.DropRule{TargetServerId: tid, DropRate: rate})
		}
	}

	_, err = pb.NewAdminServiceClient(conn).SetDropRules(context.Background(), &pb.SetDropRulesRequest{Rules: pbRules})
	if err != nil {
		log.Fatalf("set drop rules: %v", err)
	}
	fmt.Println("OK drop rules updated")
}

// ============================================================
// Scenario runners
// ============================================================

var (
	addrs = []string{"localhost:5001", "localhost:5002", "localhost:5003"}
)

func runScenario(name string) {
	switch name {
	case "basic":
		scenarioBasic()
	case "duplicates":
		scenarioDuplicates()
	case "failover":
		scenarioFailover()
	case "recovery":
		scenarioRecovery()
	case "partition":
		scenarioPartition()
	case "loadtest":
		scenarioLoadTest()
	default:
		fmt.Printf("Unknown scenario: %s\n", name)
		os.Exit(1)
	}
}

func mustDial(addr string) *grpc.ClientConn {
	conn, err := dial(addr)
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

func header(msg string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("  %s\n", msg)
	fmt.Println(strings.Repeat("=", 60))
}

func step(msg string) {
	fmt.Printf("\n>>> %s\n", msg)
}

func scenarioBasic() {
	header("Scenario: Strong Consistency")

	step("Connecting to all 3 nodes...")
	c1 := pb.NewKVServiceClient(mustDial(addrs[0]))
	c2 := pb.NewKVServiceClient(mustDial(addrs[1]))
	c3 := pb.NewKVServiceClient(mustDial(addrs[2]))

	step("PUT x=1 via node1")
	resp, err := c1.Put(context.Background(), &pb.PutRequest{
		Key: "x", Value: "1", ClientId: "scenario-basic", OpId: 1,
	})
	if err != nil || resp.Error != "" {
		log.Fatalf("put failed: err=%v resp=%v", err, resp)
	}
	fmt.Println("  OK")

	time.Sleep(200 * time.Millisecond)

	step("GET x via node2")
	gr2, err := c2.Get(context.Background(), &pb.GetRequest{
		Key: "x", ClientId: "scenario-basic-r2", OpId: 1,
	})
	if err != nil {
		log.Fatalf("get node2: %v", err)
	}
	fmt.Printf("  node2: x=%s (found=%v)\n", gr2.Value, gr2.Found)

	step("GET x via node3")
	gr3, err := c3.Get(context.Background(), &pb.GetRequest{
		Key: "x", ClientId: "scenario-basic-r3", OpId: 1,
	})
	if err != nil {
		log.Fatalf("get node3: %v", err)
	}
	fmt.Printf("  node3: x=%s (found=%v)\n", gr3.Value, gr3.Found)

	step("PUT y=hello via node2, APPEND y+=_world via node3")
	c2Put, _ := c2.Put(context.Background(), &pb.PutRequest{
		Key: "y", Value: "hello", ClientId: "scenario-basic", OpId: 2,
	})
	fmt.Printf("  put y=hello: error=%q\n", c2Put.GetError())

	c3App, _ := c3.Append(context.Background(), &pb.AppendRequest{
		Key: "y", Value: "_world", ClientId: "scenario-basic", OpId: 3,
	})
	fmt.Printf("  append y+=_world: error=%q\n", c3App.GetError())

	step("GET y via node1 (should be hello_world)")
	gr1, _ := c1.Get(context.Background(), &pb.GetRequest{
		Key: "y", ClientId: "scenario-basic-r1", OpId: 2,
	})
	fmt.Printf("  node1: y=%s (found=%v)\n", gr1.Value, gr1.Found)

	step("DELETE x via node3")
	dr, _ := c3.Delete(context.Background(), &pb.DeleteRequest{
		Key: "x", ClientId: "scenario-basic", OpId: 4,
	})
	fmt.Printf("  delete x: found=%v\n", dr.Found)

	step("GET x via node1 (should be not found)")
	gr1x, _ := c1.Get(context.Background(), &pb.GetRequest{
		Key: "x", ClientId: "scenario-basic-r1", OpId: 3,
	})
	fmt.Printf("  node1: x=%s (found=%v)\n", gr1x.Value, gr1x.Found)

	if gr2.Value == "1" && gr3.Value == "1" && gr1.Value == "hello_world" && !gr1x.Found {
		header("PASS: All reads consistent across replicas")
	} else {
		header("FAIL: Inconsistency detected!")
	}
}

func scenarioDuplicates() {
	header("Scenario: Duplicate Suppression (Idempotency)")

	conn := mustDial(addrs[0])
	c := pb.NewKVServiceClient(conn)
	admin := pb.NewAdminServiceClient(conn)

	// Get baseline metrics.
	mBefore, _ := admin.Metrics(context.Background(), &pb.MetricsRequest{})

	step("Sending 100 PUT requests with SAME (clientID, opID)...")
	clientID := fmt.Sprintf("dedup-test-%d", time.Now().UnixNano())
	opID := int64(42)

	successCount := 0
	for i := 0; i < 100; i++ {
		resp, err := c.Put(context.Background(), &pb.PutRequest{
			Key: "dedup-key", Value: "dedup-value", ClientId: clientID, OpId: opID,
		})
		if err == nil && resp.Error == "" {
			successCount++
		}
	}

	step("Checking metrics...")
	mAfter, _ := admin.Metrics(context.Background(), &pb.MetricsRequest{})

	suppBefore := mBefore.Counters["duplicate_suppressed_total"]
	suppAfter := mAfter.Counters["duplicate_suppressed_total"]
	newSuppressions := suppAfter - suppBefore

	applBefore := mBefore.Counters["applied_ops_total"]
	applAfter := mAfter.Counters["applied_ops_total"]
	newApplied := applAfter - applBefore

	fmt.Printf("  Requests sent:          100\n")
	fmt.Printf("  Successful responses:    %d\n", successCount)
	fmt.Printf("  New applied writes:      %d\n", newApplied)
	fmt.Printf("  New duplicates caught:   %d\n", newSuppressions)
	fmt.Printf("  Suppression rate:        %.0f%%\n",
		float64(newSuppressions)/99.0*100.0)

	step("Verifying key has correct value (single write)")
	gr, _ := c.Get(context.Background(), &pb.GetRequest{
		Key: "dedup-key", ClientId: "dedup-reader", OpId: time.Now().UnixNano(),
	})
	fmt.Printf("  dedup-key=%s\n", gr.Value)

	if newSuppressions >= 90 {
		header("PASS: Duplicate suppression working (>=90% caught)")
	} else {
		header("FAIL: Expected ~99 suppressions")
	}
}

func scenarioFailover() {
	header("Scenario: Failover and Uptime")

	step("Verifying all 3 nodes are healthy...")
	for _, addr := range addrs {
		conn := mustDial(addr)
		admin := pb.NewAdminServiceClient(conn)
		h, err := admin.Health(context.Background(), &pb.HealthRequest{})
		if err != nil {
			log.Fatalf("node %s unhealthy: %v", addr, err)
		}
		fmt.Printf("  Node %d: healthy=%v\n", h.ServerId, h.Healthy)
		conn.Close()
	}

	step("Writing via node1: failover-key=before")
	conn1 := mustDial(addrs[0])
	c1 := pb.NewKVServiceClient(conn1)
	c1.Put(context.Background(), &pb.PutRequest{
		Key: "failover-key", Value: "before", ClientId: "failover", OpId: 1,
	})

	step("*** STOP NODE 3 NOW (Ctrl+C on its terminal), then press Enter here ***")
	fmt.Print("  Press Enter when node3 is stopped: ")
	fmt.Scanln()

	step("Writing via node1 with only 2/3 nodes up: failover-key=after")
	resp, err := c1.Put(context.Background(), &pb.PutRequest{
		Key: "failover-key", Value: "after", ClientId: "failover", OpId: 2,
	})
	if err != nil || resp.Error != "" {
		fmt.Printf("  WARNING: put may have failed: err=%v resp=%v\n", err, resp)
	} else {
		fmt.Println("  OK — write succeeded with 2/3 nodes!")
	}

	step("Reading via node2: failover-key")
	conn2 := mustDial(addrs[1])
	c2 := pb.NewKVServiceClient(conn2)
	gr, _ := c2.Get(context.Background(), &pb.GetRequest{
		Key: "failover-key", ClientId: "failover-reader", OpId: time.Now().UnixNano(),
	})
	fmt.Printf("  node2: failover-key=%s\n", gr.Value)

	step("*** RESTART NODE 3 NOW, then press Enter ***")
	fmt.Print("  Press Enter when node3 is restarted: ")
	fmt.Scanln()

	time.Sleep(3 * time.Second) // Let catch-up happen.

	step("Reading via node3 (should have caught up): failover-key")
	conn3 := mustDial(addrs[2])
	c3 := pb.NewKVServiceClient(conn3)
	gr3, _ := c3.Get(context.Background(), &pb.GetRequest{
		Key: "failover-key", ClientId: "failover-reader3", OpId: time.Now().UnixNano(),
	})
	fmt.Printf("  node3: failover-key=%s\n", gr3.Value)

	if gr.Value == "after" && gr3.Value == "after" {
		header("PASS: Failover and recovery successful")
	} else {
		header("FAIL: Data inconsistency after failover")
	}
}

func scenarioRecovery() {
	header("Scenario: Lagging Replica Recovery via Consensus Replays")

	step("*** STOP NODE 2 NOW, then press Enter ***")
	fmt.Print("  Press Enter when node2 is stopped: ")
	fmt.Scanln()

	conn1 := mustDial(addrs[0])
	c1 := pb.NewKVServiceClient(conn1)

	step("Writing 10 entries via node1 while node2 is down...")
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("recovery-%d", i)
		val := fmt.Sprintf("value-%d", i)
		resp, err := c1.Put(context.Background(), &pb.PutRequest{
			Key: key, Value: val, ClientId: "recovery-writer", OpId: int64(100 + i),
		})
		if err != nil || resp.Error != "" {
			fmt.Printf("  WARN: put %s failed\n", key)
		} else {
			fmt.Printf("  wrote %s=%s\n", key, val)
		}
	}

	step("Checking node1 health...")
	admin1 := pb.NewAdminServiceClient(conn1)
	h1, _ := admin1.Health(context.Background(), &pb.HealthRequest{})
	fmt.Printf("  Node1: chosenIndex=%d lastApplied=%d\n", h1.ChosenIndex, h1.LastAppliedIndex)

	step("*** RESTART NODE 2 NOW, then press Enter ***")
	fmt.Print("  Press Enter when node2 is restarted: ")
	fmt.Scanln()

	time.Sleep(5 * time.Second) // Let catch-up run.

	step("Checking node2 health after catch-up...")
	conn2 := mustDial(addrs[1])
	admin2 := pb.NewAdminServiceClient(conn2)
	h2, _ := admin2.Health(context.Background(), &pb.HealthRequest{})
	fmt.Printf("  Node2: chosenIndex=%d lastApplied=%d\n", h2.ChosenIndex, h2.LastAppliedIndex)

	step("Verifying data on node2...")
	c2 := pb.NewKVServiceClient(conn2)
	allMatch := true
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("recovery-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		gr, _ := c2.Get(context.Background(), &pb.GetRequest{
			Key: key, ClientId: "recovery-reader", OpId: time.Now().UnixNano(),
		})
		match := gr.Value == expected
		if !match {
			allMatch = false
		}
		fmt.Printf("  node2: %s=%s (expected=%s, match=%v)\n", key, gr.Value, expected, match)
	}

	step("Checking catch-up metrics on node2...")
	m2, _ := admin2.Metrics(context.Background(), &pb.MetricsRequest{})
	fmt.Printf("  catchup_entries_applied: %d\n", m2.Counters["catchup_entries_applied"])

	if allMatch {
		header("PASS: Lagging replica recovered all data")
	} else {
		header("FAIL: Data mismatch on recovered replica")
	}
}

func scenarioPartition() {
	header("Scenario: Partition Tolerance")

	step("Setting drop rules: node1 drops all messages to node2")
	conn1 := mustDial(addrs[0])
	admin1 := pb.NewAdminServiceClient(conn1)
	admin1.SetDropRules(context.Background(), &pb.SetDropRulesRequest{
		Rules: []*pb.DropRule{{TargetServerId: 2, DropRate: 1.0}},
	})

	step("Setting drop rules: node2 drops all messages to node1")
	conn2 := mustDial(addrs[1])
	admin2 := pb.NewAdminServiceClient(conn2)
	admin2.SetDropRules(context.Background(), &pb.SetDropRulesRequest{
		Rules: []*pb.DropRule{{TargetServerId: 1, DropRate: 1.0}},
	})

	step("Majority side (node1+node3) should still work...")
	c1 := pb.NewKVServiceClient(conn1)
	resp, err := c1.Put(context.Background(), &pb.PutRequest{
		Key: "partition-key", Value: "majority-side", ClientId: "partition", OpId: 1,
	})
	if err != nil || resp.Error != "" {
		fmt.Printf("  Majority write result: err=%v resp=%v\n", err, resp)
	} else {
		fmt.Println("  Majority side write: OK")
	}

	step("Minority side (node2 alone) should NOT be able to commit...")
	c2 := pb.NewKVServiceClient(conn2)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	resp2, err := c2.Put(ctx, &pb.PutRequest{
		Key: "partition-key-minority", Value: "should-fail", ClientId: "partition-minority", OpId: 1,
	})
	cancel()
	if err != nil {
		fmt.Printf("  Minority write failed (expected): %v\n", err)
	} else if resp2.Error != "" {
		fmt.Printf("  Minority write error (expected): %s\n", resp2.Error)
	} else {
		fmt.Println("  WARNING: Minority write succeeded unexpectedly")
	}

	step("Healing partition: clearing drop rules on both nodes")
	admin1.SetDropRules(context.Background(), &pb.SetDropRulesRequest{})
	admin2.SetDropRules(context.Background(), &pb.SetDropRulesRequest{})

	time.Sleep(3 * time.Second) // Let catch-up happen.

	step("Verifying convergence: reading partition-key from all nodes")
	for _, addr := range addrs {
		conn := mustDial(addr)
		c := pb.NewKVServiceClient(conn)
		gr, _ := c.Get(context.Background(), &pb.GetRequest{
			Key: "partition-key", ClientId: "partition-verify", OpId: time.Now().UnixNano(),
		})
		h, _ := pb.NewAdminServiceClient(conn).Health(context.Background(), &pb.HealthRequest{})
		fmt.Printf("  %s (node %d): partition-key=%s\n", addr, h.ServerId, gr.Value)
		conn.Close()
	}

	header("Scenario complete. Verify majority writes succeeded and nodes converged.")
}

func scenarioLoadTest() {
	header("Scenario: Concurrent Load Test")

	step("Running 10 concurrent clients, 20 ops each...")
	var wg sync.WaitGroup
	var totalOps atomic.Int64
	var errors atomic.Int64

	for c := 0; c < 10; c++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			addr := addrs[clientNum%len(addrs)]
			conn, err := dial(addr)
			if err != nil {
				errors.Add(20)
				return
			}
			defer conn.Close()
			kvc := pb.NewKVServiceClient(conn)
			clientID := fmt.Sprintf("loadtest-client-%d", clientNum)

			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("lt-%d-%d", clientNum, i)
				val := fmt.Sprintf("v%d", rand.Intn(1000))
				resp, err := kvc.Put(context.Background(), &pb.PutRequest{
					Key: key, Value: val, ClientId: clientID, OpId: int64(i + 1),
				})
				if err != nil || resp.Error != "" {
					errors.Add(1)
				} else {
					totalOps.Add(1)
				}
			}
		}(c)
	}

	wg.Wait()

	fmt.Printf("\n  Total successful ops: %d\n", totalOps.Load())
	fmt.Printf("  Errors: %d\n", errors.Load())

	step("Verifying consistency across nodes...")
	// Read a sample of keys from all nodes and check they agree.
	mismatches := 0
	for c := 0; c < 10; c++ {
		key := fmt.Sprintf("lt-%d-0", c)
		var values []string
		for _, addr := range addrs {
			conn := mustDial(addr)
			kvc := pb.NewKVServiceClient(conn)
			gr, _ := kvc.Get(context.Background(), &pb.GetRequest{
				Key: key, ClientId: "loadtest-verify", OpId: time.Now().UnixNano(),
			})
			values = append(values, gr.Value)
			conn.Close()
		}
		allSame := true
		for _, v := range values[1:] {
			if v != values[0] {
				allSame = false
			}
		}
		if !allSame {
			mismatches++
			fmt.Printf("  MISMATCH: %s -> %v\n", key, values)
		}
	}

	step("Checking final metrics...")
	for _, addr := range addrs {
		conn := mustDial(addr)
		h, _ := pb.NewAdminServiceClient(conn).Health(context.Background(), &pb.HealthRequest{})
		fmt.Printf("  %s (node %d): chosenIndex=%d lastApplied=%d\n",
			addr, h.ServerId, h.ChosenIndex, h.LastAppliedIndex)
		conn.Close()
	}

	if mismatches == 0 {
		header("PASS: All sampled keys consistent across replicas")
	} else {
		header(fmt.Sprintf("FAIL: %d mismatches detected", mismatches))
	}
}

// startNode is a helper for automated scenarios (not used in interactive mode).
func startNode(id int, port int, peers string) *exec.Cmd {
	cmd := exec.Command("./bin/dkv-node",
		"--id", fmt.Sprintf("%d", id),
		"--addr", fmt.Sprintf(":%d", port),
		"--peers", peers,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("start node %d: %v", id, err)
	}
	return cmd
}
