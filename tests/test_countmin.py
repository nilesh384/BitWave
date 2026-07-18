import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from windowed_sketch_engine import CountMinSketch

def test_countmin_frequency_estimate():
    cms = CountMinSketch(width=2000, depth=5)
    for _ in range(500):
        cms.add("iphone")
    for _ in range(50):
        cms.add("samsung")

    # CMS can only overestimate due to collisions, never underestimate
    assert cms.estimate("iphone") >= 500
    assert cms.estimate("samsung") >= 50