# Demonstration Guide

This guide walks through each scenario demonstrating the distributed KV store's properties.

## Prerequisites

Build the binaries:

```bash
make build    # or: make build-only (if proto already generated)
```

Clean any previous data:

```bash
rm -rf data/
```

## Starting the Cluster

Open three separate terminals and start a 3-node cluster:

```bash
# Terminal 1
./bin/dkv-node --id 1 --addr :5001 --peers "2=localhost:5002,3=localhost:5003"

# Terminal 2
./bin/dkv-node --id 2 --addr :5002 --peers "1=localhost:5001,3=localhost:5003"

# Terminal 3
./bin/dkv-node --id 3 --addr :5003 --peers "1=localhost:5001,2=localhost:5002"
```

---

## Scenario 1: Strong Consistency

Demonstrates that writes on one node are immediately visible (linearizable) on all other nodes.

```bash
./bin/dkv-cli scenario basic
```

**What happens:**
1. PUT x=1 via node1
2. GET x via node2 and node3 — both return "1"
3. PUT y=hello via node2, APPEND y+=_world via node3
4. GET y via node1 — returns "hello_world"
5. DELETE x via node3, GET x via node1 — not found

**Expected output:**
```
============================================================
  Scenario: Strong Consistency
============================================================

>>> PUT x=1 via node1
  OK

>>> GET x via node2
  node2: x=1 (found=true)

>>> GET x via node3
  node3: x=1 (found=true)

>>> PUT y=hello via node2, APPEND y+=_world via node3
  put y=hello: error=""
  append y+=_world: error=""

>>> GET y via node1 (should be hello_world)
  node1: y=hello_world (found=true)

>>> DELETE x via node3
  delete x: found=true

>>> GET x via node1 (should be not found)
  node1: x= (found=false)

============================================================
  PASS: All reads consistent across replicas
============================================================
```

---

## Scenario 2: Duplicate Suppression (Idempotency)

Demonstrates that retrying the same operation 100 times only applies it once.

```bash
./bin/dkv-cli scenario duplicates
```

**What happens:**
1. Sends 100 PUT requests with the **same** (clientID, opID) to node1
2. Only the first request actually executes; the other 99 are detected as duplicates
3. Metrics show ~99% suppression rate

**Expected output:**
```
============================================================
  Scenario: Duplicate Suppression (Idempotency)
============================================================

>>> Sending 100 PUT requests with SAME (clientID, opID)...

>>> Checking metrics...
  Requests sent:          100
  Successful responses:    100
  New applied writes:      1
  New duplicates caught:   99
  Suppression rate:        100%

>>> Verifying key has correct value (single write)
  dedup-key=dedup-value

============================================================
  PASS: Duplicate suppression working (>=90% caught)
============================================================
```

---

## Scenario 3: Failover and Uptime

**Interactive** — requires stopping and restarting a node manually.

```bash
./bin/dkv-cli scenario failover
```

**Steps:**
1. Verifies all 3 nodes are healthy
2. Writes failover-key=before via node1
3. **Prompts you to stop node3** (Ctrl+C in terminal 3)
4. Writes failover-key=after via node1 — succeeds with 2/3 nodes
5. Reads via node2 — returns "after"
6. **Prompts you to restart node3**
7. Waits for catch-up, then reads via node3 — returns "after"

**Expected:** Write succeeds with majority (2/3). After restart, node3 catches up and serves correct reads.

---

## Scenario 4: Lagging Replica Recovery

**Interactive** — requires stopping and restarting a node manually.

```bash
./bin/dkv-cli scenario recovery
```

**Steps:**
1. **Prompts you to stop node2**
2. Writes 10 key-value pairs via node1 (while node2 is down)
3. Shows node1's chosenIndex and lastApplied
4. **Prompts you to restart node2**
5. Waits for catch-up (background FetchChosen loop)
6. Reads all 10 keys from node2 — all match expected values
7. Shows catch-up metrics

**Expected:** All 10 keys match. `catchup_entries_applied` metric shows entries were fetched from peers.

---

## Scenario 5: Partition Tolerance

Simulates a network partition using drop rules (no manual intervention needed).

```bash
./bin/dkv-cli scenario partition
```

**What happens:**
1. Sets drop rules: node1 drops all messages to node2, and vice versa
2. Node1+node3 form the majority — writes succeed on this side
3. Node2 alone is the minority — writes timeout/fail (can't reach majority)
4. Clears drop rules (heals partition)
5. After catch-up, all 3 nodes converge on the same data

**Expected output:**
```
============================================================
  Scenario: Partition Tolerance
============================================================

>>> Setting drop rules: node1 drops all messages to node2
>>> Setting drop rules: node2 drops all messages to node1

>>> Majority side (node1+node3) should still work...
  Majority side write: OK

>>> Minority side (node2 alone) should NOT be able to commit...
  Minority write error (expected): propose failed: paxos: too many retries

>>> Healing partition: clearing drop rules on both nodes

>>> Verifying convergence: reading partition-key from all nodes
  localhost:5001 (node 1): partition-key=majority-side
  localhost:5002 (node 2): partition-key=majority-side
  localhost:5003 (node 3): partition-key=majority-side

============================================================
  Scenario complete. Verify majority writes succeeded and nodes converged.
============================================================
```

---

## Scenario 6: Concurrent Load Test

Runs 10 concurrent clients sending 20 operations each (200 total ops), then verifies consistency.

```bash
./bin/dkv-cli scenario loadtest
```

**What happens:**
1. 10 goroutines each write 20 key-value pairs to random nodes
2. Reports success/error counts
3. Samples keys from all 3 nodes and checks they agree
4. Shows final metrics from each node

**Expected:** All sampled keys are consistent across all replicas.

---

## Manual CLI Operations

```bash
# Write a key
./bin/dkv-cli put --addr localhost:5001 --key greeting --value "hello world"

# Read from a different node
./bin/dkv-cli get --addr localhost:5002 --key greeting

# Check health
./bin/dkv-cli health --addr localhost:5001
./bin/dkv-cli health --addr localhost:5002
./bin/dkv-cli health --addr localhost:5003

# View metrics
./bin/dkv-cli metrics --addr localhost:5001

# Inspect the replicated log
./bin/dkv-cli fetch-log --addr localhost:5001

# Fault injection: make node1 drop messages to node2
./bin/dkv-cli drop-rules --addr localhost:5001 --rules "2:1.0"

# Clear fault injection
./bin/dkv-cli drop-rules --addr localhost:5001 --rules clear
```
