# Real-Time Customer Intelligence Platform

A real-time streaming analytics engine that estimates unique visitors and
trending products using probabilistic data structures — **HyperLogLog** and
**Count-Min Sketch** — implemented from scratch and wired into a live
Kafka → Postgres → Streamlit pipeline.

Instead of storing millions of raw user IDs to answer "how many unique
visitors in the last hour?", this engine estimates it using a fixed-size
sketch of ~1KB, with under 1-2% error — and does the same for "what's
trending right now" without keeping a full frequency dictionary.

🔴 **[Live demo](https://demostandalonepy-8yzvwkjvhsgeqbgmkdswdx.streamlit.app/)**
— engine-only, zero setup. Synthetic traffic is generated in-process for
this demo so anyone can view it instantly without running Docker/Kafka. It
uses the exact same `WindowedSketchEngine` / `HyperLogLog` / `CountMinSketch`
code as the full pipeline below — only the transport layer (Kafka) is
skipped for the hosted demo. For the full real Kafka → Postgres →
Streamlit pipeline, clone the repo and follow "Running it locally" below.

---

## Why this exists

Most student data projects wire together Kafka + Spark + a pretrained model
and call it "real-time." The actual hard part of real-time analytics —
keeping **bounded memory** while answering unbounded streaming queries — is
usually skipped by just using a database `COUNT(DISTINCT ...)` under the
hood, which doesn't scale.

This project implements the underlying technique that real systems (Redis
`PFCOUNT`, Apache Druid, Elasticsearch cardinality aggregations) actually use
to solve this, from first principles, and benchmarks it against the naive
exact approach with real numbers.

---

## Architecture

```
┌──────────────┐   ┌────────┐   ┌───────────────────────────────┐   ┌────────────┐   ┌────────────┐
│  Producer     │──▶│ Kafka  │──▶│   Windowed Sketch Engine        │──▶│  Postgres   │──▶│  Streamlit  │
│  (simulated   │   │(Docker)│   │   - HyperLogLog (uniques)       │   │  (sink /    │   │  Dashboard  │
│  events)      │   │        │   │   - Count-Min Sketch (frequency)│   │  warehouse) │   │  (live)     │
└──────────────┘   └────────┘   │   - Top-K tracker                │   └────────────┘   └────────────┘
                                  │   - Tumbling-bucket windowing    │
                                  └───────────────────────────────┘
```

**Data flow:** a producer simulates user click/purchase events with a
realistic skewed distribution → events are published to a Kafka topic → a
consumer reads the stream and updates a custom-built windowed sketch engine
→ every 10 seconds, a snapshot (estimated unique users + top trending items)
is persisted to Postgres → a Streamlit dashboard reads from Postgres and
auto-refreshes.

---

## The core engine — how it works

### The problem with exact counting
Tracking "unique users in the last hour" naively means keeping a `set()` of
every user ID seen — memory grows linearly with traffic, and doing this per
segment (per product, per region, per store) multiplies that cost.

### HyperLogLog (unique count estimation)
Hashes each item, tracks the maximum number of leading zero bits observed
across `m` registers, and combines them with a bias-corrected harmonic mean.
Fixed memory (`m` bytes) regardless of how many unique items are seen.
Standard error ≈ `1.04/√m`.

### Count-Min Sketch + Top-K tracker (frequency / trending items)
A 2D array of counters updated via multiple independent hash functions;
querying an item's frequency takes the **minimum** across its counters
(collisions can only inflate, never deflate, an estimate). Paired with a
small candidate tracker to surface the current top-K trending items.

### Windowed engine (the actual novel piece)
Neither structure supports "unwinding" old data on its own. This project
solves sliding-window queries by keeping **tumbling time buckets**, each
with its own HLL + CMS, and merging the relevant buckets on read (both
structures are mergeable — register-wise max for HLL, element-wise sum for
CMS). Old buckets are evicted once they fall outside the maximum tracked
window. This is the same bucket-and-merge pattern used by Redis and Druid
internally for approximate windowed aggregation.

Full write-up of the design and implementation: [`docs/streaming-aggregation-engine-guide.md`](docs/streaming-aggregation-engine-guide.md)

---

## Benchmark results (run on my own machine, see `benchmark/`)

**Single global counter, 1M unique users:**

| | Exact (`set()`) | HyperLogLog |
|---|---|---|
| Memory | 84,526 KB | 1.06 KB |
| Error | — | 0.28% – 4.4% (varies by run) |

**1,000 parallel segments (realistic use case — e.g. per-store/per-region counters), ~2,000 events each:**

| | Exact (`set()` per segment) | HyperLogLog per segment |
|---|---|---|
| Total memory | 87.34 MB | 1.03 MB |
| Memory reduction | — | **~85x smaller** |
| Aggregate error | — | 0.15% |
| Merging all segments into one global total | 87.88 MB result, 0.12s | **1.06 KB result (constant), 0.04s** |

The headline result isn't raw single-counter speed (Python's `set()` is a
highly optimized C structure and will usually still win on a single count in
pure Python) — it's that **memory per segment stays fixed regardless of
scale**, and **merge results stay constant-size regardless of how much data
went in**, which is what makes arbitrarily many parallel real-time counters,
and cheap merges across them, actually feasible.

Reproduce these yourself:
```bash
cd benchmark
uv run python benchmark.py               # single global counter
uv run python benchmark_multisegment.py  # 1000 parallel segments
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python |
| Streaming | Apache Kafka (Docker, KRaft mode) |
| Storage | PostgreSQL (Docker) |
| Dashboard | Streamlit + Plotly |
| Package management | [uv](https://github.com/astral-sh/uv) |
| Core engine | Built from scratch — no external sketch library |

Entirely free / open-source — no paid services required.

---

## Project structure

```
customer-intelligence-platform/
├── engine/
│   ├── hyperloglog.py       # HyperLogLog implementation
│   ├── countmin.py          # Count-Min Sketch implementation
│   ├── topk.py               # Top-K heavy-hitters tracker
│   └── windowed_engine.py    # Tumbling-bucket windowed engine
├── producer/
│   └── simulate_events.py    # Simulated event stream -> Kafka
├── consumer/
│   └── run_engine.py         # Kafka consumer -> engine -> Postgres sink
├── dashboard/
│   ├── app.py                  # Streamlit live dashboard (reads from Postgres, full pipeline)
│   └── demo_standalone.py      # Zero-setup public demo (in-process synthetic traffic, no Kafka/Postgres)
├── benchmark/
│   ├── benchmark.py                 # exact vs HLL, single counter
│   └── benchmark_multisegment.py    # exact vs HLL, 1000 parallel segments
├── sql/
│   └── schema.sql             # Postgres schema
├── docs/
│   └── streaming-aggregation-engine-guide.md
├── docker-compose.yml         # Kafka + Postgres
└── README.md
```

---

## Running it locally

**Requirements:** Docker Desktop, [uv](https://github.com/astral-sh/uv)

```bash
git clone <your-repo-url>
cd customer-intelligence-platform

# 1. Start Kafka + Postgres
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Start the producer (Terminal 1)
cd producer && uv run python simulate_events.py

# 4. Start the consumer (Terminal 2)
cd consumer && uv run python run_engine.py

# 5. Start the dashboard (Terminal 3)
cd dashboard && uv run streamlit run app.py
```

Dashboard will be live at `http://localhost:8501`.

---

## Roadmap

- **V1 (current):** Single-machine engine, HLL + CMS + Top-K, tumbling
  1-min buckets, Kafka → Postgres → Streamlit, fully benchmarked in Python.
- **V2 (planned): rewrite the core engine in Go.** The Python prototype
  proved the algorithm and the windowing design; V2 keeps the exact same
  logic (HLL, CMS, Top-K, tumbling-bucket merge-on-read) but reimplements it
  in Go for real throughput — compiled, goroutine-based concurrent
  ingestion, and a much smaller memory footprint per running instance than
  the Python interpreter overhead. Goal: publish a head-to-head benchmark
  (events/sec, memory, p99 latency) of the Python engine vs. the Go engine
  on identical synthetic traffic. Also adds: conservative-update Count-Min
  Sketch (reduces over-estimation bias), and approximate percentile
  tracking (t-digest/KLL) for real-time order-value percentiles.
- **V3 (planned):** Partitioned engine across multiple Kafka consumer
  groups with per-partition sketches merged at query time — horizontal
  scaling of the same mergeable-sketch property used for windowing.
  Package the Go engine as a small standalone binary/microservice with a
  gRPC or HTTP query API, so the Python side (dashboard, ML, AI agent) just
  calls it over the network instead of embedding it.

---

## Contributing / extending this

This project is intentionally structured so each layer is swappable and
easy to poke at independently:

- **Swap the sketch parameters:** `HyperLogLog(b=...)` and
  `CountMinSketch(width=..., depth=...)` are both plain constructor args —
  try different values in `benchmark/` and see the accuracy/memory tradeoff
  curve yourself.
- **Swap the window size:** `bucket_seconds` and `max_window_buckets` in
  `WindowedSketchEngine` control granularity and history depth — no other
  code needs to change.
- **Add a new sketch type:** the engine treats HLL/CMS as pluggable
  per-bucket state; adding a new mergeable sketch (e.g. t-digest for
  percentiles) means adding one more field to the bucket tuple and merging
  it the same way in `query_window()`.
- **Swap the language for the core engine:** the algorithms in
  `engine/` are deliberately kept free of Python-specific tricks (no
  numpy vectorization, no comprehension magic) so they map cleanly to a
  port in Go/Rust/C++ — see the V2 roadmap above.

Issues and PRs welcome — in particular, benchmark contributions from
different hardware, or a Go/Rust port of `engine/`, are exactly the kind of
thing this project could use.

---

## What I learned building this

- Why probabilistic data structures trade a small, bounded error for
  massive memory savings, and *why* that tradeoff is usually worth it
- Why single sketches can't answer sliding-window queries on their own, and
  how time-bucketing + merge-on-read solves that
- The real performance tradeoff between Python's C-optimized `set()` vs a
  pure-Python sketch implementation — and where the *actual* wins are
  (many parallel counters + cheap merges, not raw single-counter speed)
- End-to-end streaming infrastructure: Kafka producer/consumer patterns,
  decoupling ingestion from persistence, and building a live dashboard on
  top of a continuously-updating data source

---