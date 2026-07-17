import hashlib
import heapq
import random


class CountMinSketch:
    """Probabilistic data structure to estimate item frequencies with minimal memory."""

    def __init__(self, width=2000, depth=5):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
        # Unique seeds to ensure independent hashing per row
        self.seeds = [i * 0x9E3779B1 for i in range(depth)]

    def _hash(self, item: str, seed: int) -> int:
        h = hashlib.md5(f"{seed}-{item}".encode("utf-8")).digest()
        return int.from_bytes(h[:4], "big") % self.width

    def add(self, item: str, count: int = 1):
        for row in range(self.depth):
            col = self._hash(item, self.seeds[row])
            self.table[row][col] += count

    def estimate(self, item: str) -> int:
        # The minimum value across rows filters out hash collision noise
        return min(
            self.table[row][self._hash(item, self.seeds[row])]
            for row in range(self.depth)
        )


class TopKTracker:
    """Maintains a real-time list of the top K heaviest hitters using a fixed-size Min-Heap."""

    def __init__(self, k=10):
        self.k = k
        self.heap = []  # Elements stored as tuples: (estimated_count, item)
        self.items_in_heap = set()  # Quick lookup to prevent duplicate items in heap

    @property
    def candidates(self):
        """Compatibility alias for code that expects the tracker to expose candidate items."""
        return self.items_in_heap

    def observe(self, item: str, cms: CountMinSketch):
        # 1. Get the freshest frequency estimate from CMS
        current_estimate = cms.estimate(item)

        # 2. If the item is already in our heavy-hitters club, we must update its score
        if item in self.items_in_heap:
            # Rebuild heap with the updated count for this specific item
            self.heap = [(cms.estimate(i), i) for count, i in self.heap]
            heapq.heapify(self.heap)
            return

        # 3. If the heap isn't full yet, directly admit the item
        if len(self.heap) < self.k:
            heapq.heappush(self.heap, (current_estimate, item))
            self.items_in_heap.add(item)
            return

        # 4. If heap is full, challenge the lowest-ranking heavy hitter (the root of the min-heap)
        min_count, min_item = self.heap[0]
        if current_estimate > min_count:
            # Evict the old loser
            heapq.heappop(self.heap)
            self.items_in_heap.remove(min_item)

            # Insert the new heavy hitter
            heapq.heappush(self.heap, (current_estimate, item))
            self.items_in_heap.add(item)

    def top_k(self) -> list:
        """Returns the sorted top-K elements from highest to lowest frequency."""
        # The heap contains the correct elements, but sorting it guarantees descending order for the user
        return sorted(self.heap, key=lambda x: x[0], reverse=True)


if __name__ == "__main__":
    # Initialize our streaming analytics engine
    cms = CountMinSketch(width=2000, depth=5)
    topk = TopKTracker(k=5)

    # Define a highly skewed dataset (simulating real internet traffic distributions)
    items = ["iphone", "samsung", "pixel", "oneplus", "nokia", "xiaomi"]
    weights = [50, 30, 10, 5, 3, 2]  # iphone is vastly more popular

    print("Processing 10,000 stream events...")
    for _ in range(10_000):
        item = random.choices(items, weights=weights)[0]

        # Log to Count-Min Sketch (tracks frequencies anonymously)
        cms.add(item)

        # Feed to TopKTracker (actively decides if it qualifies as a trending item)
        topk.observe(item, cms)

    # Display final results
    print("\n--- Final Real-Time Leaderboard ---")
    for rank, (count, item) in enumerate(topk.top_k(), 1):
        print(f"Rank {rank}: {item:<10} (Estimated Occurrences: {count})")
