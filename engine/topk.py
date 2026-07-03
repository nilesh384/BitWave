import heapq

class TopKTracker:
    def __init__(self, k=10):
        self.k = k
        self.candidates = {}  # item -> last known estimated count

    def observe(self, item: str, cms):
        self.candidates[item] = cms.estimate(item)
        if len(self.candidates) > self.k * 5:
            self._trim()

    def _trim(self):
        top = heapq.nlargest(self.k, self.candidates.items(), key=lambda kv: kv[1])
        self.candidates = dict(top)

    def top_k(self, cms):
        refreshed = {item: cms.estimate(item) for item in self.candidates}
        return heapq.nlargest(self.k, refreshed.items(), key=lambda kv: kv[1])


if __name__ == "__main__":
    from countmin import CountMinSketch
    import random

    cms = CountMinSketch(width=2000, depth=5)
    topk = TopKTracker(k=5)

    items = ["iphone", "samsung", "pixel", "oneplus", "nokia", "xiaomi"]
    for _ in range(2000):
        item = random.choices(items, weights=[50, 30, 10, 5, 3, 2])[0]
        cms.add(item)
        topk.observe(item, cms)

    print("Top 5 trending:", topk.top_k(cms))