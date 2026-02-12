# Distributed Key-Value Store

A high-performance, fault-tolerant distributed key-value store built using Paxos for consensus. I built this project out of personal interest to deepen my understanding of distributed systems, specifically the Paxos consensus algorithm, and to gain hands-on experience with implementing fault-tolerant systems. This project is a result of my curiosity and passion for distributed computing.Feel free to clone this repository, explore the code, and use it as a learning resource or a foundation for your own projects. Contributions and feedback are always welcome!

## Features

- **Distributed Consensus**: Implements Paxos to ensure consistency across nodes.
- **Fault Tolerance**: Handles node failures gracefully while maintaining data integrity.
- **gRPC API**: Provides a client-facing API for key-value operations.
- **Pluggable Storage**: Uses BoltDB for persistent storage, with an interface for extending to other backends.
- **Linearizable Reads**: Ensures strong consistency for read operations.
- **Scalable Design**: Easily add or remove nodes in the cluster.

## Architecture

The system is composed of the following components:

1. **Paxos Engine**: Manages consensus for operations using Paxos instances.
2. **gRPC Services**: Exposes APIs for `Get`, `Put`, and other operations.
3. **Storage Layer**: Provides persistent storage for key-value data.
4. **Node Management**: Handles communication between nodes and manages cluster state.

## Getting Started

### Prerequisites

- Go 1.24 or higher
- Make
- Protobuf Compiler

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd distributed-kv-store
   ```

2. Install dependencies:
   ```bash
   make deps
   ```

3. Compile the protobuf definitions:
   ```bash
   make proto
   ```

### Running the System

1. Start a node:
   ```bash
   go run cmd/node/main.go --id=1 --addr=:5001 --peers=2=localhost:5002,3=localhost:5003
   ```

2. Start additional nodes with unique IDs and addresses.

3. Use the CLI to interact with the cluster:
   ```bash
   go run cmd/cli/main.go --action=put --key=mykey --value=myvalue
   go run cmd/cli/main.go --action=get --key=mykey
   ```

### Testing

Run the integration and unit tests:
```bash
make test
```

## File Structure

- `cmd/`: Contains the CLI and node entry points.
- `internal/`: Core logic for Paxos, server, and storage.
- `proto/`: Protobuf definitions and generated code.
- `integration/`: Integration tests for the cluster.
- `scripts/`: Utility scripts.

## Commands to Create and Push Repository

1. Initialize a Git repository:
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   ```

2. Create a new repository on GitHub:
   ```bash
   gh repo create distributed-kv-store --public --source=. --push
   ```

3. Push the code:
   ```bash
   git push -u origin main
   ```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
