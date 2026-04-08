# Distributed KV Store

A distributed key-value store built in Go, designed for fault tolerance and high availability.

## Architecture and Design


At its core, this system leverages the **Raft consensus algorithm** to guarantee distributed agreement across the cluster. To optimize performance without the overhead of a traditional database engine, the key-value store operates entirely in-memory. Data durability and fault tolerance are preserved by maintaining an immutable, sequential ledger of state transitions known as the Write-Ahead Log (WAL).

**Core Mechanisms:**
1. **Leader Election:** Every node initializes as a `Follower`. If a follower fails to receive a heartbeat (an `AppendEntries` RPC call) from an active leader within a randomized timeout window, it transitions to a `Candidate` state and initiates a new election. The cluster then converges to elect a single `Leader` for the active term.
2. **Log Replication:** To maintain stringent consistency, all write operations (`PUT`, `DELETE`) are routed exclusively to the leader. The leader appends the proposed mutation to its local WAL and subsequently broadcasts it to all follower nodes via `AppendEntries` gRPC calls.
3. **Commit & Application:** A log entry achieves a "committed" state only after the leader has successfully replicated it to a quorum (majority) of the cluster. Once committed, the leader and followers systematically apply the mutation to their underlying in-memory state machines, rendering the key-value pair active for read queries.
4. **Crash Recovery:** In the event of a crash or planned restart, a node autonomously reconstructs its previous state by sequentially replaying the historical operations securely persisted in its on-disk WAL.

### System Components
- **API Engine:** Manages incoming HTTP REST traffic. Read queries are served locally, while write mutations are transparently redirected to the authoritative Raft leader.
- **State Machine:** A highly optimized in-memory map holding the operational key-value records.
- **Consensus Module:** Orchestrates the lifecycle of Raft elections, handles log replication, and manages seamless gRPC interoperability among the distributed nodes.
- **Write-Ahead Log (WAL):** A reliable, append-only persistence layer that securely captures system mutations sequentially onto a local `.log` file before they are enacted in memory.

## Codebase Overview

- `main.go`: The central entry point. Responsible for parsing CLI arguments (Node ID, exposed ports, and peer topologies) to bootstrap the server configuration and initiate the node lifecycle.
- `internal/api/`:
  - `handlers.go`: Exposes REST endpoints (`GET`, `PUT`, `DELETE`). Ensures transactional safety by routing mutation requests to the active leader while allowing localized reads directly from the state machine.
  - `router.go` & `server.go`: Composes the HTTP request multiplexer and initializes the HTTP daemon.
- `internal/proto/raft/`: Hosts the gRPC Protocol Buffers definitions alongside auto-generated Go bindings. Defines Raft's fundamental RPCs: `AppendEntries` and `RequestVote`.
- `internal/server/store/`: The domain core that bridges the consensus algorithm with the data storage layer.
  - `store.go`: The primary struct encompassing the node's Raft state, WAL subsystem, and the in-memory map. Governs the promotion of committed entries into the state machine.
  - `raft.go`: Handles processing logic for incoming `AppendEntries` and `RequestVote` RPC invocations. Explicitly enforces cluster consistency and orchestrates leader election timer mechanics.
  - `followers.go`: Facilitates the outward synchronization process, allowing the leader to proactively stream and replicate log entries out to the follower node pool.
- `internal/server/wal/`:
  - `wal.go`: A custom Write-Ahead Log engine. Manages precise binary serialization, atomic disk appends, log recovery reads, and targeted truncations to reliably ensure data persistence.

## Features

- **Distributed Consensus**: Leader election and log replication.
- **Write-Ahead Logging (WAL)**: Ensures data durability and state recovery across restarts.
- **HTTP API**: Simple REST-like endpoints for interacting with the key-value store. (to be replaced!)

## Getting Started


### Running Nodes

Start the first node:

```bash
go run main.go -id 1 -port 8080 -path ./tmp
```

Start additional nodes and specify the peers:

```bash
go run main.go -id 2 -port 8081 -path ./tmp -peers "1=localhost:8080"
go run main.go -id 3 -port 8082 -path ./tmp -peers "1=localhost:8080,2=localhost:8081"
```

*Note that nodes will automatically run elections and agree on a consensus leader.*

## HTTP API

The API exposes endpoints to manage key-value pairs:

- **Get a value:** `GET /value?key=<key>`
- **Put a value:** `PUT /value` 
  - Body: `{"key": "your_key", "value": "your_value"}`
  - *Writes must be directed to the current leader.*
- **Delete a value:** `DELETE /value?key=<key>`
  - *Deletes must be directed to the current leader.*

## Upcoming Features

- **Consistent Hashing**: To distribute keys more smoothly and efficiently across the cluster, avoiding major redistributions when nodes are added or removed.
- **Data Partitioning**: Splitting the dataset into multiple partitions to scale horizontal read and write capacity beyond a single node's limits.
