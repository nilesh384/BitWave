import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from windowed_sketch_engine import CountMinSketch, TopKTracker

def test_topk_ranks_correctly():
    cms = CountMinSketch(width=2000, depth=5)
    topk = TopKTracker(k=3)

    items_with_weight = {"iphone": 100, "samsung": 50, "pixel": 20, "nokia": 1}
    for item, weight in items_with_weight.items():
        for _ in range(weight):
            cms.add(item)
            topk.observe(item, cms)

    result = topk.top_k()
    ranked_items = [item for count, item in result]
    assert ranked_items[0] == "iphone"
    assert "nokia" not in ranked_items  # should be evicted, k=3 only fits top 3