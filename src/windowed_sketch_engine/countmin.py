import hashlib


class CountMinSketch:
    def __init__(self, width=2000, depth=5):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
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