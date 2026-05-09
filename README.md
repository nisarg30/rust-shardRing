# rust-shardRing

**Lock-free market data distribution via sharded ring buffers over shared memory.**

A Rust library for broadcasting real-time market data to multiple strategy instances running on the same machine — using a single inbound socket and zero-copy shared memory instead of one socket per consumer. Built for latency-critical trading infrastructure where every microsecond counts.

---

## The Problem

When multiple algorithmic trading strategies run on the same machine, the naive approach is to open a separate socket connection per strategy to receive market data. This creates:

- **N × network overhead** — duplicate data delivery for every strategy instance
- **Lock contention** — shared queues protected by mutexes serialize access and introduce jitter
- **Head-of-line blocking** — a slow strategy can stall data flow for all others
- **Copy overhead** — data is deserialized and heap-allocated separately for each consumer

rust-shardRing solves all of these.

---

## How It Works

### Single Inbound Connection

A single socket (or equivalent transport) receives market data from the exchange/feed. All strategies on the machine are consumers of this one connection. Network overhead is O(1) regardless of how many strategies are running.

### Shared Memory via Memory-Mapped Files

Incoming data is written into a memory-mapped file (`memmap2`). All strategy processes map the same file into their address space, so data is never copied — every consumer reads directly from the same physical memory pages the producer wrote to.

### Sharded Ring Buffers

The shared memory region is divided into **shards** — independent ring buffers, each covering a subset of instruments. Instrument tokens are deterministically assigned to shards via consistent hashing, so a given token always lands in the same shard. This means:

- The producer never needs a lock to route incoming data — the shard is determined by the token at compile time.
- Consumers reading a specific instrument only touch that instrument's shard and never contend with consumers of other shards.
- No mutex, no spinlock, no atomic CAS loop for the common read path.

### Two Access Modes

**Latest-only access** — A consumer that only needs the most recent tick for an instrument reads the head of the shard directly. This is a single memory read with no coordination.

**Sequential (0-miss) access** — A consumer that needs every update in order tracks its own read cursor in the ring buffer and advances it independently of other consumers. Because ring buffers are allocated with `heapless` (stack-resident, no heap allocation), sequential reads are cache-friendly and branch-free.

### Backpressure via Shard Pressure Balancing

When a shard's ring buffer fills up (a slow consumer isn't keeping up), the library redistributes pressure across shards rather than blocking the producer or dropping data silently. The sharding layout is monitored and rebalanced so that hot instruments don't starve slower-moving ones, giving consumers time to drain without cascading stalls.

---

## Architecture

```
         Exchange Feed
               │
         (single socket)
               │
         ┌─────▼──────┐
         │  Producer  │
         └─────┬──────┘
               │  token → shard (hash, no lock)
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌───────┐  ┌───────┐  ┌───────┐
│Shard 0│  │Shard 1│  │Shard 2│   ← ring buffers in mmap'd file
└───┬───┘  └───┬───┘  └───┬───┘
    │           │           │
    └───────────┼───────────┘
                │  (shared memory, zero-copy)
    ┌───────────┼───────────┐
    ▼           ▼           ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│Strategy A│ │Strategy B│ │Strategy C│
│(latest)  │ │(seq. all)│ │(latest)  │
└──────────┘ └──────────┘ └──────────┘
```

Each strategy process maps the same file. Each reads independently with its own cursor. No locks cross the producer–consumer boundary.

---

## Dependencies

| Crate | Purpose |
|---|---|
| `memmap2` | Memory-mapped file for zero-copy shared memory |
| `heapless` | Stack-allocated ring buffer storage — no heap, cache-friendly |
| `hashbrown` | Fast hash map for token → shard routing table |
| `bytes` | Efficient byte buffer management for raw tick data |
| `threadpool` | Worker thread pool for concurrent shard writes |

---

## Use Cases

- **Multi-strategy trading engines** where several algos run on the same box and need the same tick stream
- **Risk monitors** that shadow live strategies and need real-time position/PnL updates without being in the critical path
- **Backtesting replay servers** that need to fan out historical data to many parallel simulation workers
- **Market microstructure research** requiring per-tick access to order book updates across hundreds of instruments simultaneously

---

## Getting Started

### Prerequisites

- Rust 2021 edition or later (`rustup update stable`)
- Linux or macOS (shared memory-mapped files; Windows support may vary)

### Add as a dependency

```toml
[dependencies]
rust-data-distribution = { git = "https://github.com/nisarg30/rust-shardRing" }
```

### Build from source

```bash
git clone https://github.com/nisarg30/rust-shardRing
cd rust-shardRing
cargo build --release
```

---

## Design Properties

| Property | Guarantee |
|---|---|
| Producer locking | None — shard routing is deterministic by token |
| Consumer locking | None — each consumer owns its read cursor |
| Memory copies | Zero — consumers read directly from mmap'd pages |
| Heap allocation on hot path | None — `heapless` ring buffers are stack-resident |
| Slow consumer isolation | Shard pressure balancing prevents cross-shard stalls |
| Latest-tick latency | Single memory read |
| Sequential-tick latency | Ring buffer advance, no coordination |

---

## License

MIT

---

*Built with Rust for latency-critical, high-throughput market data infrastructure.*
