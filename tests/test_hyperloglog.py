import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from windowed_sketch_engine import HyperLogLog

def test_hyperloglog_accuracy():
    hll = HyperLogLog(b=10)
    for i in range(100_000):
        hll.add(f"user_{i}")
    estimate = hll.count()
    error = abs(estimate - 100_000) / 100_000
    assert error < 0.05  # within 5%, generous margin over the ~3.25% theoretical std error

def test_hyperloglog_merge():
    hll1 = HyperLogLog(b=10)
    hll2 = HyperLogLog(b=10)
    for i in range(50_000):
        hll1.add(f"user_{i}")
    for i in range(50_000, 100_000):
        hll2.add(f"user_{i}")
    hll1.merge(hll2)
    estimate = hll1.count()
    error = abs(estimate - 100_000) / 100_000
    assert error < 0.05