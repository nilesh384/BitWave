import time
from collections import deque
import heapq

from hyperloglog import HyperLogLog
from countmin import CountMinSketch
from topk import TopKTracker


class WindowedSketchEngine:
    def __init__(self, bucket_seconds=60, max_window_buckets=60,
                 hll_b=10, cms_w=2000, cms_d=5):
        self.bucket_seconds = bucket_seconds
        self.max_window_buckets = max_window_buckets
        self.hll_b, self.cms_w, self.cms_d = hll_b, cms_w, cms_d
        self.buckets = deque()  # (bucket_start_ts, HLL, CMS, TopKTracker)

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
        now = now if now is not None else time.time()
        cutoff = now - window_seconds
        merged_hll = HyperLogLog(self.hll_b)
        merged_cms = CountMinSketch(self.cms_w, self.cms_d)
        merged_candidates = {}

        for bucket_start, hll, cms, topk in self.buckets:
            if bucket_start >= cutoff:
                merged_hll.merge(hll)
                for r in range(self.cms_d):
                    for c in range(self.cms_w):
                        merged_cms.table[r][c] += cms.table[r][c]
                merged_candidates.update(topk.candidates)

        unique_estimate = merged_hll.count()
        top_items = heapq.nlargest(
            5,
            ((item, merged_cms.estimate(item)) for item in merged_candidates),
            key=lambda kv: kv[1],
        )
        return {"unique_users_estimate": unique_estimate, "top_items": top_items}


if __name__ == "__main__":
    import random

    engine = WindowedSketchEngine(bucket_seconds=5, max_window_buckets=12)  # 5s buckets, 60s max window
    products = ["iphone", "samsung", "pixel", "oneplus"]

    # simulate 30 seconds of live events
    start = time.time()
    for i in range(300):
        user = f"user_{random.randint(1, 150)}"
        item = random.choices(products, weights=[50, 30, 15, 5])[0]
        ts = start + (i * 0.1)  # spread events over ~30 seconds
        engine.record_event(user, item, ts)

    result = engine.query_window(window_seconds=30, now=start + 30)
    print("Last 30s window:")
    print("Unique users:", result["unique_users_estimate"])
    print("Top items:", result["top_items"])
    
    # Test: query a SHORT window (should show fewer users than the full 30s window)
    short_result = engine.query_window(window_seconds=10, now=start + 30)
    print("\nLast 10s window (should be smaller):")
    print("Unique users:", short_result["unique_users_estimate"])