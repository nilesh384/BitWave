# Project: Real-Time Customer Intelligence Platform
## Core Component — Probabilistic Streaming Aggregation Engine (built from scratch)

This is the piece that makes your project stand out: instead of storing raw
user IDs to count unique visitors, or a full dictionary to find trending
products, you'll implement your own **windowed sketch-based state store**
using HyperLogLog (HLL) and Count-Min Sketch (CMS) — the same class of data
structures used inside Redis, Spark, Druid, and Elasticsearch's cardinality
aggregations.

In the hybrid version of this repo, this engine is the piece that moves to
Go first. Python stays in place for orchestration, dashboarding, and
benchmarking; the Go service owns the hot path and exposes the same sketch
semantics over a small API.

The pitch, in one line: *"I built a streaming engine that estimates unique
users and trending items over sliding time windows using O(kilobytes) of
memory instead of O(millions) of raw records."*

---

## 1. Why this matters (the theory, in plain terms)

### The problem with exact counting
If you want "unique visitors in the last 1 hour," the naive approach is to
keep a `set()` of every user ID seen in that hour. For 10 million events with
high cardinality, that's tens to hundreds of MB per window — and you need one
per metric, per segment (per product, per region...). It doesn't scale.

### HyperLogLog — approximate distinct count
**Core idea:** hash each item to a uniform random bit string. The *position
of the first 1-bit* (leading/trailing zeros) in a hash is a random variable —
seeing a hash with many leading zeros is rare, and the more distinct items
you've hashed, the more likely you are to have seen a "rare" one. By tracking
the *maximum* number of leading zeros observed, you can estimate cardinality.

A single such estimator is noisy, so HLL splits the hash space into `m`
buckets (using the first few bits of the hash to pick the bucket, the rest to
compute leading zeros), keeps the max-leading-zeros-seen per bucket in a
register array, then combines all `m` registers with a **harmonic mean**
(harmonic mean punishes outliers, which controls variance).

- **Memory:** `m` single bytes (e.g., `m = 1024` → 1 KB) regardless of
  whether you've seen 1,000 or 100,000,000 unique items.
- **Accuracy:** standard error ≈ `1.04 / sqrt(m)`. At `m = 1024`, that's
  ~3.25% error — usually fine for dashboards/business metrics.
- **Key property — mergeable:** two HLLs can be merged by taking the
  register-wise max. This is *exactly* what makes windowing possible (see
  §3).

### Count-Min Sketch — approximate frequency / heavy hitters
**Core idea:** a 2D array of counters with `d` rows and `w` columns, each row
associated with an independent hash function. To add an item, hash it `d`
times (once per row) and increment the counter at each `(row, hash(item) mod
w)` position. To estimate an item's frequency, take the **minimum** across
its `d` counters — collisions can only inflate a counter, never deflate it,
so the min is the best available estimate.

- **Memory:** `w × d` counters (e.g., `w=2000, d=5` → 10,000 integers ≈ 40
  KB), independent of the number of distinct items.
- **Trending items / heavy hitters:** CMS alone gives you a frequency
  *estimate for a known item*, but not "what are the top items" — for that
  you pair CMS with a small **min-heap of K candidates**, refreshing counts
  from the sketch as new items arrive (implemented in §4).
- **Mergeable** the same way as HLL: element-wise sum of the counter arrays.

---

## 2. System architecture

```
┌─────────────┐   ┌───────────┐   ┌──────────────────────────────┐   ┌─────────────┐
│  Event       │   │           │   │   YOUR STREAMING ENGINE       │   │  Warehouse   │
│  Producer    │──▶│  Kafka    │──▶│  (this guide)                 │──▶│  Postgres /  │
│  (faker sim) │   │  topic    │   │  - Tumbling 1-min buckets      │   │  BigQuery    │
└─────────────┘   └───────────┘   │  - HLL per bucket (uniques)    │   └──────┬──────┘
                                    │  - CMS per bucket (frequency)  │          │
                                    │  - Sliding-window merge on read│          ▼
                                    └──────────────┬─────────────────┘   ┌─────────────┐
                                                    │                     │  ML models   │
                                                    ▼                     │  (churn, rec,│
                                          ┌─────────────────┐             │  anomaly)    │
                                          │  Streamlit       │◀────────────┤              │
                                          │  dashboard +      │             └─────────────┘
                                          │  NL query agent   │
                                          └─────────────────┘
```

Everything to the left and right of the middle box is what you already
planned (Kafka producer, warehouse, ML, dashboard). In the hybrid setup,
that middle box becomes a Go service, while Python keeps the surrounding
workflow and UI.

---

## 3. The windowing design — why buckets, not one giant sketch

You can't just keep *one* HLL running forever — that gives you "unique users
since the app started," not "unique users in the last hour." And you can't
recompute the sketch by re-reading all raw events either — that defeats the
memory-saving purpose.

**The standard trick (used by Redis, Druid, etc.): time-bucketed mergeable
sketches.**

1. Divide time into fixed **tumbling buckets** (e.g., 1-minute buckets).
2. Maintain one small HLL + CMS *per bucket*, only for currently "live"
   buckets (e.g., last 60 buckets = last hour).
3. When someone asks "unique users in the last hour," **merge** the last 60
   buckets on the fly (register-wise max for HLL, element-wise sum for CMS)
   — this is cheap and doesn't mutate the stored buckets.
4. Evict buckets older than your max window (e.g., >60 minutes old) to keep
   memory bounded forever, no matter how long the stream runs.

This is the real engineering insight to highlight in your write-up: **you're
trading exact recomputation for approximate mergeability**, which is what
makes arbitrary sliding windows (5-min, 1-hr, 24-hr — all from the same
bucket store) cheap to serve.

---

## 4. Implementation

### 4.1 HyperLogLog

```python
import hashlib
import math

class HyperLogLog:
    def __init__(self, b=10):
        """
        b: number of bits used to index registers.
        m = 2^b registers. b=10 -> m=1024 registers -> ~3.25% std error, 1KB memory.
        """
        self.b = b
        self.m = 1 << b
        self.registers = bytearray(self.m)  # each register: max leading-zero-run+1 seen
        # bias-correction constant (standard HLL alpha for m >= 128)
        self.alpha = 0.7213 / (1 + 1.079 / self.m)

    def _hash(self, item: str) -> int:
        h = hashlib.sha1(item.encode("utf-8")).digest()
        return int.from_bytes(h[:8], "big")  # 64-bit hash

    def add(self, item: str):
        x = self._hash(item)
        j = x & (self.m - 1)              # first b bits -> bucket index
        w = x >> self.b                   # remaining bits -> used for rank
        rank = self._rho(w)
        if rank > self.registers[j]:
            self.registers[j] = rank

    def _rho(self, w: int, max_bits: int = 64 - 10) -> int:
        """Position of leftmost 1-bit in w (i.e. 1 + number of leading zeros)."""
        if w == 0:
            return max_bits + 1
        return max_bits - w.bit_length() + 1

    def count(self) -> float:
        Z = sum(2.0 ** -r for r in self.registers)
        raw_estimate = self.alpha * self.m * self.m / Z

        # small-range correction (linear counting) when there are many empty registers
        zeros = self.registers.count(0)
        if raw_estimate <= 2.5 * self.m and zeros > 0:
            return self.m * math.log(self.m / zeros)
        return raw_estimate

    def merge(self, other: "HyperLogLog"):
        assert self.m == other.m, "Cannot merge HLLs of different size"
        for i in range(self.m):
            if other.registers[i] > self.registers[i]:
                self.registers[i] = other.registers[i]

    def copy(self) -> "HyperLogLog":
        clone = HyperLogLog(self.b)
        clone.registers = bytearray(self.registers)
        return clone
```

### 4.2 Count-Min Sketch (with a top-K heavy-hitters tracker)

```python
import hashlib
import heapq

class CountMinSketch:
    def __init__(self, width=2000, depth=5):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
        # one distinct seed per row -> d independent hash functions
        self.seeds = [i * 0x9E3779B1 for i in range(depth)]

    def _hash(self, item: str, seed: int) -> int:
        h = hashlib.md5(f"{seed}-{item}".encode("utf-8")).digest()
        return int.from_bytes(h[:4], "big") % self.width

    def add(self, item: str, count: int = 1):
        for row in range(self.depth):
            col = self._hash(item, self.seeds[row])
            self.table[row][col] += count

    def estimate(self, item: str) -> int:
        return min(
            self.table[row][self._hash(item, self.seeds[row])]
            for row in range(self.depth)
        )

    def merge(self, other: "CountMinSketch"):
        for r in range(self.depth):
            for c in range(self.width):
                self.table[r][c] += other.table[r][c]


class TopKTracker:
    """
    CMS gives frequency of a KNOWN item, not 'what are the top items'.
    Pair it with a bounded candidate set refreshed against the sketch.
    """
    def __init__(self, k=10):
        self.k = k
        self.candidates = {}  # item -> last known estimated count

    def observe(self, item: str, cms: CountMinSketch):
        self.candidates[item] = cms.estimate(item)
        if len(self.candidates) > self.k * 5:  # cap candidate set size
            self._trim()

    def _trim(self):
        top = heapq.nlargest(self.k, self.candidates.items(), key=lambda kv: kv[1])
        self.candidates = dict(top)

    def top_k(self, cms: CountMinSketch):
        # refresh counts at read-time for accuracy
        refreshed = {item: cms.estimate(item) for item in self.candidates}
        return heapq.nlargest(self.k, refreshed.items(), key=lambda kv: kv[1])
```

### 4.3 The windowed bucket manager (the actual "engine")

```python
import time
from collections import deque

class WindowedSketchEngine:
    def __init__(self, bucket_seconds=60, max_window_buckets=60, hll_b=10, cms_w=2000, cms_d=5):
        self.bucket_seconds = bucket_seconds
        self.max_window_buckets = max_window_buckets
        self.hll_b, self.cms_w, self.cms_d = hll_b, cms_w, cms_d
        # deque of (bucket_start_ts, HLL, CMS, TopKTracker)
        self.buckets = deque()

    def _current_bucket_start(self, ts):
        return int(ts // self.bucket_seconds) * self.bucket_seconds

    def _get_or_create_bucket(self, ts):
        bucket_start = self._current_bucket_start(ts)
        if self.buckets and self.buckets[-1][0] == bucket_start:
            return self.buckets[-1]
        new_bucket = (bucket_start, HyperLogLog(self.hll_b),
                      CountMinSketch(self.cms_w, self.cms_d), TopKTracker())
        self.buckets.append(new_bucket)
        self._evict_old(ts)
        return new_bucket

    def _evict_old(self, ts):
        cutoff = self._current_bucket_start(ts) - self.max_window_buckets * self.bucket_seconds
        while self.buckets and self.buckets[0][0] < cutoff:
            self.buckets.popleft()

    def record_event(self, user_id: str, item_id: str, ts: float = None):
        ts = ts if ts is not None else time.time()
        _, hll, cms, topk = self._get_or_create_bucket(ts)
        hll.add(user_id)
        cms.add(item_id)
        topk.observe(item_id, cms)

    def query_window(self, window_seconds: int, now: float = None):
        """Merge all buckets within the last `window_seconds` and return estimates."""
        now = now if now is not None else time.time()
        cutoff = now - window_seconds
        merged_hll = HyperLogLog(self.hll_b)
        merged_cms = CountMinSketch(self.cms_w, self.cms_d)
        merged_topk_candidates = {}

        for bucket_start, hll, cms, topk in self.buckets:
            if bucket_start >= cutoff:
                merged_hll.merge(hll)
                merged_cms.merge(cms)
                merged_topk_candidates.update(topk.candidates)

        unique_estimate = merged_hll.count()
        top_items = heapq.nlargest(
            10,
            ((item, merged_cms.estimate(item)) for item in merged_topk_candidates),
            key=lambda kv: kv[1],
        )
        return {"unique_users_estimate": unique_estimate, "top_items": top_items}
```

### 4.4 Wiring it into your Kafka consumer

```python
from kafka import KafkaConsumer
import json

engine = WindowedSketchEngine(bucket_seconds=60, max_window_buckets=60)  # up to 1hr windows

consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

for message in consumer:
    event = message.value  # {"user_id": ..., "item_id": ..., "timestamp": ...}
    engine.record_event(event["user_id"], event["item_id"], event["timestamp"])

    # every N events (or on a timer thread) push a snapshot to Postgres/BigQuery
    # for the dashboard to read, e.g.:
    # snapshot = engine.query_window(3600)  # last hour
    # write_to_warehouse(snapshot)
```

Run the warehouse write on a **separate timer thread** (e.g., every 10
seconds) rather than every event — that mirrors how real systems separate
the hot ingestion path from the slower persistence path.

---

## 5. Benchmark it — this is what makes the post credible

Build a small benchmark script comparing your engine against the naive exact
approach:

```python
import sys
import random
import string

def random_id():
    return "".join(random.choices(string.ascii_letters + string.digits, k=12))

N = 1_000_000
user_ids = [random_id() for _ in range(N)]

# --- Exact ---
exact_set = set()
for uid in user_ids:
    exact_set.add(uid)
exact_count = len(exact_set)
exact_memory = sys.getsizeof(exact_set) + sum(sys.getsizeof(u) for u in exact_set)

# --- HyperLogLog ---
hll = HyperLogLog(b=10)
for uid in user_ids:
    hll.add(uid)
hll_count = hll.count()
hll_memory = sys.getsizeof(hll.registers)

error_pct = abs(hll_count - exact_count) / exact_count * 100

print(f"Exact:  {exact_count:>10}  |  Memory: {exact_memory/1024:.1f} KB")
print(f"HLL:    {hll_count:>10.0f}  |  Memory: {hll_memory/1024:.1f} KB")
print(f"Error:  {error_pct:.2f}%  |  Memory saved: {(1 - hll_memory/exact_memory)*100:.1f}%")
```

On 1M unique IDs, expect roughly:
- Exact set: several tens of MB
- HLL (m=1024): ~1 KB
- Error: within a few percent

**Publish this table.** "8000x memory reduction for 3% error" is the exact
kind of concrete number that made the LinkedIn post credible.

Also benchmark **accuracy vs. m** (try b=8, 10, 12, 14) and **CMS accuracy
vs. width/depth** — a small chart of error-vs-memory tradeoff curves is
genuinely publication-quality content for a build-in-public post.

---

## 6. How this plugs into the rest of your platform

| Layer | What changes |
|---|---|
| **Kafka producer** | No change — still simulates events with `faker` |
| **Streaming engine** | Replace naive `pandas.groupby` aggregation with `WindowedSketchEngine` |
| **Warehouse** | Store periodic snapshots (`window_seconds`, `unique_users_estimate`, `top_items`, `computed_at`) as a new fact table, e.g. `fact_realtime_metrics` |
| **ML models** | Unchanged — churn/rec/anomaly still train on your batch warehouse tables. You can optionally feed the *real-time* unique-visitor trend as a feature (e.g., "sudden drop in unique visitors this hour" as an anomaly signal) |
| **Dashboard** | Add a live-updating panel: "Unique visitors (last 5 min / 1 hr / 24 hr)" and "Trending products right now" pulling from `fact_realtime_metrics` |
| **AI agent** | Extend your text-to-SQL agent's schema context to include the new table, so users can ask "how many unique visitors in the last hour?" in plain English |

---

## 7. Roadmap (mirror the V1/V2/V3 structure from the post you liked)

- **V1 (MVP):** Single-machine engine, in-process Kafka consumer, HLL for
  uniques + CMS for top items, snapshots pushed to Postgres every 10s,
  Streamlit dashboard reading from Postgres.
- **V2:** Add **Count-Min Sketch with conservative update** (a known
  refinement that reduces over-estimation bias), add a second sketch type —
  **t-digest or KLL sketch** for approximate percentiles (e.g., p95 order
  value in real time). Move engine to run as a standalone service consuming
  from Kafka independently of the dashboard.
- **V3:** Partition the engine across multiple consumers (Kafka consumer
  groups) with per-partition sketches merged at query time — this is a real
  distributed-systems concept (partial aggregation + merge) and mirrors how
  Spark/Druid actually scale these sketches horizontally.

---

## 8. Build-in-public post ideas (in order)

1. "Why I'm building my own streaming aggregator instead of just using
   Spark" — the conceptual motivation (memory bound vs exact answers).
2. "Implementing HyperLogLog from the SHA-1 hash up" — the math intuition
   (leading zeros → rarity → cardinality) with a diagram.
3. "The subtlety of windowed sketches: why you can't just keep one running
   HLL" — the tumbling-bucket + merge-on-read design (§3).
4. "Benchmarked my engine: 8000x less memory, 3% error" — the results table
   from §5, with a chart.
5. "V1 is live" — architecture diagram, GitHub link, demo GIF of the
   dashboard updating in real time as synthetic events stream in.

---

## 9. What to build first (suggested order)

1. Implement `HyperLogLog` standalone, test against Python's `set()` on
   random data until your error is within expected bounds.
2. Implement `CountMinSketch` + `TopKTracker` standalone, same testing
   approach.
3. Implement `WindowedSketchEngine`, test with synthetic timestamped events
   (no Kafka yet — just a Python generator).
4. Wire in Kafka producer/consumer.
5. Add the warehouse sink + Streamlit panel.
6. Run and publish the benchmark (§5).
7. Write the first build-in-public post.
