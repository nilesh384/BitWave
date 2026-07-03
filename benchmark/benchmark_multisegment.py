import sys
import os
import time
import random
import string

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "engine"))
from hyperloglog import HyperLogLog


def random_id():
    return "".join(random.choices(string.ascii_letters + string.digits, k=12))


# ---------------------------------------------------------
# SCENARIO: 1,000 independent segments (e.g. postal codes / stores / product IDs),
# each needs its OWN "unique visitors" counter running at the same time.
# This is the real shape of the problem (Uber-style: unique riders PER city,
# PER hour), not one giant global counter.
# ---------------------------------------------------------

NUM_SEGMENTS = 10_000
USERS_PER_SEGMENT = 2_000          # each segment sees ~2000 events
UNIQUE_POOL_PER_SEGMENT = 1_500    # with some repeat visits, ~1500 truly unique users

print(f"Simulating {NUM_SEGMENTS} segments, each with ~{USERS_PER_SEGMENT} events "
      f"(~{UNIQUE_POOL_PER_SEGMENT} unique users)...\n")

# Pre-generate a shared pool of user ids per segment so there ARE genuine repeats
segment_events = []
for _ in range(NUM_SEGMENTS):
    pool = [random_id() for _ in range(UNIQUE_POOL_PER_SEGMENT)]
    events = [random.choice(pool) for _ in range(USERS_PER_SEGMENT)]
    segment_events.append(events)

# --- Approach 1: one Python set() per segment ---
start = time.time()
sets_per_segment = []
for events in segment_events:
    s = set()
    for uid in events:
        s.add(uid)
    sets_per_segment.append(s)
set_time = time.time() - start

set_total_memory = sum(
    sys.getsizeof(s) + sum(sys.getsizeof(u) for u in s) for s in sets_per_segment
)
set_total_unique_sum = sum(len(s) for s in sets_per_segment)

# --- Approach 2: one HyperLogLog per segment ---
start = time.time()
hlls_per_segment = []
for events in segment_events:
    h = HyperLogLog(b=10)
    for uid in events:
        h.add(uid)
    hlls_per_segment.append(h)
hll_time = time.time() - start

hll_total_memory = sum(sys.getsizeof(h.registers) for h in hlls_per_segment)
hll_total_unique_sum = sum(h.count() for h in hlls_per_segment)


def fmt(b):
    if b >= 1024 * 1024:
        return f"{b / (1024*1024):.2f} MB"
    return f"{b / 1024:.2f} KB"


print("=" * 70)
print(f"{NUM_SEGMENTS} PARALLEL SEGMENTS — total memory across ALL counters")
print("=" * 70)
print(f"set() per segment   : {fmt(set_total_memory)}  ({set_total_memory:,} bytes)  "
      f"time={set_time:.2f}s")
print(f"HLL per segment     : {fmt(hll_total_memory)}  ({hll_total_memory:,} bytes)  "
      f"time={hll_time:.2f}s")
print()
print(f"Memory reduction    : {set_total_memory / hll_total_memory:,.0f}x smaller")
print(f"Sum of unique users : exact={set_total_unique_sum:,}  "
      f"HLL est={hll_total_unique_sum:,.0f}  "
      f"error={abs(hll_total_unique_sum - set_total_unique_sum) / set_total_unique_sum * 100:.2f}%")
print()

# ---------------------------------------------------------
# BONUS: merge cost comparison
# "unique users across ALL segments combined" (e.g. citywide total)
# ---------------------------------------------------------
print("=" * 70)
print("MERGE COST — combining all segments into one citywide total")
print("=" * 70)

start = time.time()
union_set = set()
for s in sets_per_segment:
    union_set |= s
set_merge_time = time.time() - start
print(f"set() union of all {NUM_SEGMENTS} segments : {len(union_set):,} unique, "
      f"time={set_merge_time:.4f}s, "
      f"result memory={fmt(sys.getsizeof(union_set) + sum(sys.getsizeof(u) for u in union_set))}")

start = time.time()
merged_hll = HyperLogLog(b=10)
for h in hlls_per_segment:
    merged_hll.merge(h)
hll_merge_time = time.time() - start
print(f"HLL merge of all {NUM_SEGMENTS} segments   : {merged_hll.count():,.0f} unique (est), "
      f"time={hll_merge_time:.4f}s, "
      f"result memory={fmt(sys.getsizeof(merged_hll.registers))}")

print()
print(f"Merge speedup       : {set_merge_time / hll_merge_time:.1f}x faster with HLL")
print(f"Merged result memory: constant {fmt(sys.getsizeof(merged_hll.registers))} regardless of "
      f"how many segments or how much data — set() union grows with total unique count")
print("=" * 70)

print()
print("Headline numbers for your post/resume:")
print(f"  -> Running {NUM_SEGMENTS} parallel unique-visitor counters: "
      f"{fmt(set_total_memory)} (exact) vs {fmt(hll_total_memory)} (HLL) "
      f"= {set_total_memory / hll_total_memory:,.0f}x less memory")
print(f"  -> Merging all {NUM_SEGMENTS} segments into a global total: "
      f"{set_merge_time:.3f}s (exact union) vs {hll_merge_time:.4f}s (HLL merge)")