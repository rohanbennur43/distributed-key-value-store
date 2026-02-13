# Distributed Key-Value Store with Paxos Consensus

A fault-tolerant, strongly consistent distributed key-value store implemented in Go using gRPC and classic Paxos consensus.

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Client CLI                  │
│         (Put, Get, Append, Delete)           │
└──────┬──────────────┬──────────────┬─────────┘
       │              │              │
       ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│   Node 1    │ │   Node 2    │ │   Node 3    │
│  ┌───────┐  │ │  ┌───────┐  │ │  ┌───────┐  │
│  │  KV   │  │ │  │  KV   │  │ │  │  KV   │  │
│  │Service│  │ │  │Service│  │ │  │Service│  │
│  └───┬───┘  │ │  └───┬───┘  │ │  └───┬───┘  │
│  ┌───┴───┐  │ │  ┌───┴───┐  │ │  ┌───┴───┐  │
│  │ Paxos │◄─┼─┼──│ Paxos │◄─┼─┼──│ Paxos │  │
│  │Engine │──┼─┼──►│Engine │──┼─┼──►│Engine │  │
│  └───┬───┘  │ │  └───┬───┘  │ │  └───┬───┘  │
│  ┌───┴───┐  │ │  ┌───┴───┐  │ │  ┌───┴───┐  │
│  │BoltDB │  │ │  │BoltDB │  │ │  │BoltDB │  │
│  └───────┘  │ │  └───────┘  │ │  └───────┘  │
└─────────────┘ └─────────────┘ └─────────────┘
```

Each node runs:
- **KV Service** — client-facing gRPC API (Get, Put, Append, Delete)
- **Paxos Engine** — proposer, acceptor, and learner roles for consensus
- **Admin Service** — health checks, metrics, log inspection, fault injection
- **BoltDB** — persistent storage for acceptor state, chosen log, and dedup table

### Consensus Protocol

Classic single-decree Paxos runs independently for each log index:

1. **Prepare (Phase 1a):** Proposer picks a unique proposal number `(round, serverID)` and sends `Prepare(n)` to all acceptors.
2. **Promise (Phase 1b):** Acceptors promise not to accept proposals lower than `n`, returning any previously accepted value.
3. **Accept (Phase 2a):** Proposer adopts the highest previously accepted value (if any), otherwise uses its own. Sends `Accept(n, value)`.
4. **Accepted (Phase 2b):** Acceptors accept if `n >= highestPromised`. Once a majority accepts, the value is **chosen**.
5. **Learn:** Chosen value is broadcast to all nodes and applied to the state machine.

### Linearizable Reads

Reads use a **no-op barrier** (Option B): a NOOP operation is proposed through Paxos before reading. This ensures the replica has applied all previously committed writes, providing linearizability. This trades latency for correctness — every read requires one consensus round.

### Idempotence / Duplicate Suppression

Every write carries `(clientID, opID)`. A per-client dedup table tracks `lastOpID` and returns cached results for duplicate requests. This ensures at-most-once execution even with client retries.

### Fault Tolerance

| Cluster Size | Tolerated Failures |
|:---:|:---:|
| 3 | 1 |
| 5 | 2 |

- **Crash recovery:** Acceptor state and chosen log are persisted to BoltDB with fsync.
- **Catch-up:** A periodic background loop fetches missing chosen entries from peers via `FetchChosen` RPC.
- **Partition tolerance:** The majority partition continues operating; the minority cannot commit new writes. After healing, nodes converge via catch-up.

## Quick Start

### Prerequisites

- Go 1.21+
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc` plugins

```bash
# Install protoc plugins (if not already installed)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Build

```bash
make build
```

This generates protobuf code and compiles `bin/dkv-node` and `bin/dkv-cli`.

If the proto is already generated, use `make build-only` to skip regeneration.

### Start a 3-Node Cluster

Open three terminals:

```bash
# Terminal 1
./bin/dkv-node --id 1 --addr :5001 --peers "2=localhost:5002,3=localhost:5003"

# Terminal 2
./bin/dkv-node --id 2 --addr :5002 --peers "1=localhost:5001,3=localhost:5003"

# Terminal 3
./bin/dkv-node --id 3 --addr :5003 --peers "1=localhost:5001,2=localhost:5002"
```

### Use the CLI

```bash
# Write
./bin/dkv-cli put --addr localhost:5001 --key mykey --value myvalue

# Read (from any node — linearizable)
./bin/dkv-cli get --addr localhost:5002 --key mykey

# Append
./bin/dkv-cli append --addr localhost:5001 --key mykey --value _suffix

# Delete
./bin/dkv-cli delete --addr localhost:5003 --key mykey

# Health check
./bin/dkv-cli health --addr localhost:5001

# Metrics
./bin/dkv-cli metrics --addr localhost:5001

# Inspect log
./bin/dkv-cli fetch-log --addr localhost:5001
```

### Run Scenarios

See [DEMO.md](DEMO.md) for full walkthrough.

```bash
./bin/dkv-cli scenario basic        # Strong consistency
./bin/dkv-cli scenario duplicates   # Duplicate suppression
./bin/dkv-cli scenario failover     # Crash and recovery (interactive)
./bin/dkv-cli scenario recovery     # Lagging replica catch-up (interactive)
./bin/dkv-cli scenario partition    # Network partition simulation
./bin/dkv-cli scenario loadtest     # Concurrent load test
```

## Testing

```bash
# Unit tests (Paxos engine, KV state machine)
make test

# Integration tests (in-process 3-node cluster)
make integration-test
```

## Project Structure

```
├── proto/kvstore.proto         # gRPC service definitions
├── proto/kvpb/                 # Generated protobuf Go code
├── internal/
│   ├── paxos/
│   │   ├── types.go            # ProposalNum, Operation, interfaces
│   │   ├── instance.go         # Single Paxos instance (acceptor logic)
│   │   ├── engine.go           # Multi-instance proposer + orchestration
│   │   └── engine_test.go      # Paxos unit tests
│   ├── store/
│   │   ├── kv.go               # Deterministic KV state machine + dedup
│   │   └── kv_test.go
│   ├── storage/
│   │   ├── iface.go            # Persistence interface
│   │   └── bolt.go             # BoltDB implementation
│   ├── server/
│   │   ├── node.go             # Node orchestrator (apply loop, catch-up)
│   │   ├── peer.go             # gRPC peer client + fault injection
│   │   ├── kv_service.go       # KVService gRPC implementation
│   │   ├── paxos_service.go    # PaxosService gRPC implementation
│   │   └── admin_service.go    # AdminService gRPC implementation
│   └── metrics/
│       └── metrics.go          # Atomic counters
├── cmd/node/main.go            # Node binary
├── cmd/cli/main.go             # CLI with scenarios
├── integration/
│   └── cluster_test.go         # Integration tests
├── DEMO.md                     # Demonstration guide
└── Makefile
```

## Design Tradeoffs

- **Leaderless Paxos:** Any node can propose. This simplifies the protocol but increases contention under high concurrency. A preferred-leader optimization could reduce wasted proposals.
- **NOOP read barrier:** Every read proposes a NOOP through consensus. This guarantees linearizability but adds one round-trip of latency per read. For read-heavy workloads, a lease-based approach would be more efficient.
- **BoltDB persistence:** Simple and correct with built-in fsync. Not the fastest option, but acceptable for a correctness-focused implementation.
- **Per-index Paxos:** Each log index runs an independent Paxos instance. Multi-Paxos with a stable leader would amortize the Prepare phase cost.

## Observability

Metrics exposed via the `Metrics` RPC:

| Metric | Description |
|--------|-------------|
| `applied_ops_total` | Total operations applied to state machine |
| `duplicate_suppressed_total` | Duplicate client operations detected |
| `paxos_prepare_total` | Prepare RPCs processed (acceptor) |
| `paxos_accept_total` | Accept RPCs processed (acceptor) |
| `paxos_propose_total` | Proposals initiated (proposer) |
| `chosen_index` | Highest chosen log index |
| `last_applied_index` | Highest applied log index |
| `catchup_entries_applied` | Entries fetched during catch-up |

## License

MIT
