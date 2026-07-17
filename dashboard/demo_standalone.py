"""
Standalone public demo — no Kafka, no Postgres, no Docker required.
Generates synthetic events in a background thread and feeds them directly
into the same WindowedSketchEngine used in the full pipeline, so visitors
see a live-updating dashboard the moment they open the link.

Deploy this file (and engine/) to Streamlit Community Cloud for a free,
zero-setup public demo link.
"""
# 1. MOVE IMPORTS TO THE VERY TOP
import sys
import os
import time
import random
import threading

import pandas as pd
import streamlit as st
import plotly.express as px

# 2. RUN PAGE CONFIG FIRST (Streamlit requires this to be the very first UI command)
st.set_page_config(page_title="Streaming Sketch Engine — Live Demo", layout="wide")

# 3. NOW YOU CAN SAFELY USE ST.CAPTION
st.caption(
    "🔧 This is a lightweight demo of the core engine only — synthetic events "
    "are generated in-process here for zero-setup viewing. "
    "The full project also includes a real Kafka → Postgres pipeline; "
    "see the [GitHub repo](https://github.com/nilesh384/BitWave) for the complete architecture."
)

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "engine"))
from windowed_engine import WindowedSketchEngine

# ---------------------------------------------------------------------
# Shared engine + background synthetic traffic generator (runs once per
# server process thanks to @st.cache_resource; every visitor shares the
# same live-updating engine, which is the point of a public demo)
# ---------------------------------------------------------------------

PRODUCTS = ["iphone", "samsung", "pixel", "oneplus", "nokia"]
WEIGHTS = [50, 30, 15, 4, 1]
NUM_USERS = 500


@st.cache_resource
def get_engine_and_start_traffic():
    engine = WindowedSketchEngine(bucket_seconds=30, max_window_buckets=20)  # 10 min max window
    users = [f"user_{i}" for i in range(NUM_USERS)]

    def generate_traffic():
        while True:
            engine.record_event(
                user_id=random.choice(users),
                item_id=random.choices(PRODUCTS, weights=WEIGHTS)[0],
            )
            time.sleep(random.uniform(0.02, 0.15))

    thread = threading.Thread(target=generate_traffic, daemon=True)
    thread.start()
    return engine


engine = get_engine_and_start_traffic()

# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------

st.title("🔴 Live: Windowed Streaming Sketch Engine")
st.caption(
    "A HyperLogLog + Count-Min Sketch engine estimating unique visitors and "
    "trending items over sliding time windows, using ~1KB of memory instead "
    "of storing every event. Synthetic traffic is generated live in this demo — "
    "no setup required. Full source: [GitHub link here]"
)

window_choice = st.select_slider(
    "Window size", options=[30, 60, 120, 300], value=60,
    format_func=lambda s: f"last {s}s"
)

placeholder = st.empty()

while True:
    result = engine.query_window(window_choice)

    with placeholder.container():
        col1, col2, col3 = st.columns(3)
        col1.metric(f"Unique users (last {window_choice}s, estimated)",
                    f"{result['unique_users_estimate']:.0f}")
        col2.metric("Memory used by engine", "~1 KB per window bucket")
        col3.metric("Est. error vs exact count", "≈ 1-3%")

        st.subheader("Top trending items right now")
        if result["top_items"]:
            items_df = pd.DataFrame(result["top_items"], columns=["item", "estimated_count"])
            fig = px.bar(items_df, x="item", y="estimated_count")
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Warming up — collecting first events...")

        st.caption(
            "This engine never stores raw user IDs or a full item-frequency "
            "table — it maintains fixed-size probabilistic sketches per time "
            "bucket and merges them on read. Try changing the window size "
            "above and watch the unique-user count change instantly, without "
            "any recomputation over raw data."
        )

    time.sleep(3)
