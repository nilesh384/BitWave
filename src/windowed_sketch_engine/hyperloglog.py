import mmh3
import math

class HyperLogLog:
    def __init__(self, b=10):
        self.b = b
        self.m = 1 << b
        self.registers = bytearray(self.m)
        self.alpha = 0.7213 / (1 + 1.079 / self.m)

    def _hash(self, item: str) -> int:
        h_signed = mmh3.hash64(item, signed=False)[0]  # take first 64-bit value, unsigned
        return h_signed

    def add(self, item: str):
        x = self._hash(item)
        j = x & (self.m - 1)
        w = x >> self.b
        rank = self._rho(w)
        if rank > self.registers[j]:
            self.registers[j] = rank

    def _rho(self, w: int, max_bits: int = 64 - 10) -> int:
        if w == 0:
            return max_bits + 1
        return max_bits - w.bit_length() + 1

    def count(self) -> float:
        Z = sum(2.0 ** -r for r in self.registers)
        raw_estimate = self.alpha * self.m * self.m / Z
        zeros = self.registers.count(0)
        if raw_estimate <= 2.5 * self.m and zeros > 0:
            return self.m * math.log(self.m / zeros)
        return raw_estimate

    def merge(self, other: "HyperLogLog"):
        assert self.m == other.m
        for i in range(self.m):
            if other.registers[i] > self.registers[i]:
                self.registers[i] = other.registers[i]
