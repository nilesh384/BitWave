import heapq
from .countmin import CountMinSketch  # do NOT redefine CountMinSketch here


class TopKTracker:
    """Maintains a real-time list of the top-K heaviest hitters using a fixed-size min-heap."""

    def __init__(self, k=10):
        self.k = k
        self.heap = []           # (estimated_count, item) tuples
        self.items_in_heap = {}  # item -> current estimated_count, kept in sync with heap

    @property
    def candidates(self):
        """Dict of {item: count} — used by WindowedSketchEngine to merge across buckets."""
        return self.items_in_heap

    def observe(self, item: str, cms: CountMinSketch):
        current_estimate = cms.estimate(item)

        if item in self.items_in_heap:
            # refresh this item's count in both the heap and the dict
            self.heap = [(cms.estimate(i), i) for _, i in self.heap]
            heapq.heapify(self.heap)
            self.items_in_heap[item] = current_estimate
            return

        if len(self.heap) < self.k:
            heapq.heappush(self.heap, (current_estimate, item))
            self.items_in_heap[item] = current_estimate
            return

        min_count, min_item = self.heap[0]
        if current_estimate > min_count:
            heapq.heappop(self.heap)
            del self.items_in_heap[min_item]

            heapq.heappush(self.heap, (current_estimate, item))
            self.items_in_heap[item] = current_estimate

    def top_k(self) -> list:
        """Returns the sorted top-K elements from highest to lowest frequency."""
        return sorted(self.heap, key=lambda x: x[0], reverse=True)