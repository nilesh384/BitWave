import sys
import time
import random
import string
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "engine"))
from hyperloglog import HyperLogLog


def random_id():
    return "".join(random.choices(string.ascii_letters + string.digits, k=12))


N = 1_000_000
user_ids = [random_id() for _ in range(N)]

# --- Exact (Python set) ---
start = time.time()
exact_set = set()
for uid in user_ids:
    exact_set.add(uid)
exact_time = time.time() - start
exact_count = len(exact_set)
exact_memory_bytes = sys.getsizeof(exact_set) + sum(sys.getsizeof(u) for u in exact_set)

# --- HyperLogLog ---
start = time.time()
hll = HyperLogLog(b=10)
for uid in user_ids:
    hll.add(uid)
hll_time = time.time() - start
hll_count = hll.count()
hll_memory_bytes = sys.getsizeof(hll.registers)

# --- Derived stats ---
error_pct = abs(hll_count - exact_count) / exact_count * 100
memory_ratio = exact_memory_bytes / hll_memory_bytes          # e.g. 76800x
memory_saved_pct = (1 - hll_memory_bytes / exact_memory_bytes) * 100  # will round to 100.00
speed_ratio = hll_time / exact_time                            # how much slower HLL is


def fmt_kb(b):
    return f"{b / 1024:.2f} KB"


def fmt_bytes_smart(b):
    if b >= 1024 * 1024:
        return f"{b / (1024 * 1024):.2f} MB"
    return f"{b / 1024:.2f} KB"


print("=" * 60)
print("EXACT (Python set)")
print(f"  Count      : {exact_count:,}")
print(f"  Memory     : {fmt_bytes_smart(exact_memory_bytes)} ({exact_memory_bytes:,} bytes)")
print(f"  Time       : {exact_time:.3f}s")
print()
print("HYPERLOGLOG (b=10, m=1024 registers)")
print(f"  Estimate   : {hll_count:,.0f}")
print(f"  Memory     : {fmt_bytes_smart(hll_memory_bytes)} ({hll_memory_bytes:,} bytes)")
print(f"  Time       : {hll_time:.3f}s")
print()
print("COMPARISON")
print(f"  Error            : {error_pct:.2f}%")
print(f"  Memory reduction : {memory_ratio:,.0f}x smaller  (NOT '100% saved' — that's a rounding artifact)")
print(f"  Speed            : HLL is {speed_ratio:.1f}x slower than raw set() "
      f"(hashing overhead in pure Python, not the algorithm itself)")
print("=" * 60)

# Headline stats ready to paste into a resume/LinkedIn post
print()
print("Headline numbers for your post/resume:")
print(f"  -> ~{memory_ratio:,.0f}x memory reduction ({fmt_kb(exact_memory_bytes)} -> {fmt_kb(hll_memory_bytes)})")
print(f"  -> ~{error_pct:.1f}% accuracy error at m=1024 registers")