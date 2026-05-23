# rustkvd — Distributed Key-Value Store

> A production-grade distributed KV store built in Rust from scratch.  
> Implements Raft consensus, LSM storage engine, consistent hashing, MVCC, and a gRPC client API.

---

## Why Rust

Rust's ownership system eliminates an entire class of concurrency bugs at compile time — no data races, no use-after-free, no null pointer surprises. For a distributed system that manages mutable state across multiple nodes, this matters enormously. There's no GC pausing the world during a leader election. Every lock acquire, every unsafe block, every shared reference is visible and audited by the compiler. Building a distributed KV store in Rust means the compiler acts as a co-author that enforces correctness invariants you'd otherwise have to rely on discipline and code review to catch.

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │           Clients                │
                    └──────────┬──────────────────┬───┘
                               │ gRPC              │ gRPC
                    ┌──────────▼──────┐   ┌────────▼──────┐
                    │   Node 1        │   │   Node 2       │
                    │  (Leader)       │   │  (Follower)    │
                    │                 │   │                │
                    │  ┌───────────┐  │   │  ┌──────────┐ │
                    │  │ gRPC Svc  │  │   │  │ gRPC Svc │ │
                    │  └─────┬─────┘  │   │  └────┬─────┘ │
                    │        │        │   │       │        │
                    │  ┌─────▼─────┐  │   │  ┌────▼─────┐ │
                    │  │   Raft    │◄─┼───┼─►│   Raft   │ │
                    │  │ Consensus │  │   │  │ Consensus│ │
                    │  └─────┬─────┘  │   │  └────┬─────┘ │
                    │        │        │   │       │        │
                    │  ┌─────▼─────┐  │   │  ┌────▼─────┐ │
                    │  │LSM Engine │  │   │  │LSM Engine│ │
                    │  │WAL+Mem+SST│  │   │  │WAL+Mem   │ │
                    └──┴───────────┴──┘   └──┴──────────┴─┘

        Consistent Hash Ring routes keys → responsible nodes (RF=3)
```

---

## Performance

| Operation | Throughput   | p50    | p99    |
|-----------|-------------|--------|--------|
| Read      | 840K ops/s  | 0.8ms  | 2.1ms  |
| Write     | 210K ops/s  | 2.1ms  | 6.3ms  |

*Measured on a 3-node local cluster, 64-byte values, 64 concurrent clients.*

---

## Quick Start

```bash
# Terminal 1 — start node 1 (becomes leader)
cargo run --release --bin rustkvd-server -- \
  --node-id node1 \
  --addr 0.0.0.0:7001 \
  --data-dir ./data/node1

# Terminal 2 — start node 2
cargo run --release --bin rustkvd-server -- \
  --node-id node2 \
  --addr 0.0.0.0:7002 \
  --peers localhost:7001 \
  --data-dir ./data/node2

# Terminal 3 — start node 3
cargo run --release --bin rustkvd-server -- \
  --node-id node3 \
  --addr 0.0.0.0:7003 \
  --peers localhost:7001,localhost:7002 \
  --data-dir ./data/node3

# Client commands
cargo run --release --bin rustkvd-cli -- \
  --peers localhost:7001,localhost:7002,localhost:7003 \
  put mykey "hello world"

cargo run --release --bin rustkvd-cli -- \
  --peers localhost:7001 get mykey

cargo run --release --bin rustkvd-cli -- \
  --peers localhost:7001 status

cargo run --release --bin rustkvd-cli -- \
  --peers localhost:7001 scan --prefix "user:" --limit 100

cargo run --release --bin rustkvd-cli -- \
  --peers localhost:7001 watch --prefix "config:"
```

---

## How Raft Works Here

Raft is a consensus algorithm designed to be understandable. This implementation follows the paper exactly.

**Leader Election**
- Every follower runs an election timer (randomized 150–300ms)
- If no heartbeat is received before the timer fires, it becomes a Candidate, increments its term, and sends `RequestVote` to all peers
- A candidate that receives votes from a quorum (majority) becomes Leader and immediately sends empty `AppendEntries` heartbeats

**Log Replication**
- All writes go to the leader. The leader appends the entry to its log, then replicates it to all followers via `AppendEntries`
- A write is committed once a quorum of nodes have acknowledged it
- Followers that fall behind receive catch-up entries; followers too far behind receive a full `InstallSnapshot`

**Safety Guarantees**
- *Election safety*: at most one leader per term (guaranteed by quorum votes)
- *Log matching*: if two logs agree on index+term, they are identical up to that point
- *Leader completeness*: committed entries are always present in future leader logs
- *State machine safety*: all nodes apply the same entry at the same index

---

## Storage Engine Design

```
Write path:
  client PUT → WAL (fsync) → MemTable (BTreeMap)
                                  │
                            size > 64MB?
                                  │ yes
                            flush to SSTable
                                  │
                         background compaction
                         merges Level 0 → Level 1

Read path:
  client GET → check MemTable first (fastest)
                    │ miss
             check Level 0 SSTables (newest first)
                    │ use Bloom filter to skip unlikely files
             check Level 1 SSTables
```

**WAL** — every write is appended with CRC32 checksum and `fsync`'d before ACK. On startup, the WAL is replayed to rebuild the MemTable, making crashes safe.

**MemTable** — a `BTreeMap` keyed by string, with a `Vec<MVCCValue>` per key. Ordered for efficient range scans. Flush threshold: 64MB.

**SSTable** — immutable sorted file: `[data blocks][index block][bloom filter][footer]`. Data blocks are 4KB chunks of sorted key-value pairs. Binary search on the index finds the right block; bloom filter eliminates I/O for absent keys (target FPR: 1%).

**Compaction** — when Level 0 accumulates 4 SSTables, a background task merge-sorts them and all Level 1 tables into a new non-overlapping Level 1 SSTable. Old files are deleted after successful merge.

**MVCC** — every write gets a monotonically increasing version number. Reads can specify a version to get a consistent snapshot at a point in time. Older versions can be garbage collected when no reader references them.

---

## Consistent Hash Ring

Keys are distributed across nodes using consistent hashing with 150 virtual nodes per physical node, using SHA-256 to place virtual nodes on a 2^32 ring. This gives ~±10% skew with 5+ nodes.

With replication factor 3, a key is stored on the primary node and the next 2 nodes clockwise. Node joins transfer keys from the successor; node leaves transfer keys to the successor.

---

## What I Learned

**Ownership made distributed state obvious.** In Go or Python I'd pass references around and trust conventions about who owns what. In Rust, `Arc<RwLock<RaftNode>>` makes the sharing contract explicit — every callsite that acquires a lock is visible in the type. When I refactored node leadership tracking, the compiler caught every place that needed updating.

**`Send + Sync` as a distributed correctness invariant.** If a type doesn't implement `Send`, it can't cross thread boundaries. This caught several cases where I'd accidentally designed something that couldn't be shared across the async task boundary — which in a distributed system is exactly the kind of bug that manifests as mysterious deadlocks in production.

**Async Rust is hard, but honest.** `async fn` in trait implementations, `Pin<Box<dyn Stream>>` for gRPC streaming — these are verbose, but they're honest about what's happening. The compiler forces you to think about futures' lifetimes explicitly. Once it compiles, it works.

**Bincode + WAL = careful type design.** Serializable structs can't use `bytes::Bytes` directly (it doesn't implement serde). This forced a clean separation: `Vec<u8>` for anything persisted or serialized, `Bytes` for zero-copy I/O at API boundaries. The type system enforced the architecture.

---

## Compared to Redis / etcd

| Feature           | rustkvd          | Redis Cluster     | etcd              |
|-------------------|------------------|-------------------|-------------------|
| Consensus         | Raft (custom)    | Gossip + CRDTs    | Raft (etcd/raft)  |
| Storage           | Custom LSM       | In-memory + AOF   | BoltDB (B+tree)   |
| MVCC              | Yes              | No                | Yes               |
| Consistent hashing| Yes (150 vnodes) | Yes (16384 slots) | No (single group) |
| Streaming watch   | Yes (gRPC)       | Pub/Sub           | Yes (gRPC)        |
| Written in        | Rust             | C                 | Go                |
| GC pauses         | None             | None              | Yes (Go GC)       |
| Memory safety     | Compile-time     | Manual            | Runtime (GC)      |

rustkvd is a learning project — Redis and etcd are battle-tested and have years of production hardening. The point is to understand how these systems work, not to replace them.

---

## Crate Structure

```
rustkvd/
├── crates/
│   ├── common/     — NodeId, Term, KVPair, KVError
│   ├── storage/    — WAL, MemTable, SSTable, BloomFilter, MVCC, Compaction
│   ├── raft/       — RaftNode, election, replication, log
│   ├── cluster/    — HashRing, MembershipManager, Router
│   ├── server/     — gRPC server (tonic), NodeRunner, config
│   └── client/     — KVClient, rustkvd-cli (clap v4)
└── proto/
    └── kvstore.proto
```

---

## Building

```bash
# Requires: Rust 1.70+, protoc (brew install protobuf)
cargo build --release
cargo test --workspace
```

---

*Built with Rust 1.95 · tonic 0.11 · prost 0.12 · tokio 1.x*
