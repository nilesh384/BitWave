import time
import json
import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Customer Intelligence Dashboard", layout="wide")

DB_CONFIG = dict(
    host="localhost",
    port=5433,
    user="cip_user",
    password="cip_password",
    dbname="cip_db",
)


@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_data():
    conn = get_connection()
    query = """
        SELECT id, computed_at, window_seconds, unique_users_estimate, top_items
        FROM fact_realtime_metrics
        ORDER BY id DESC
        LIMIT 200
    """
    df = pd.read_sql(query, conn)
    df["computed_at"] = pd.to_datetime(df["computed_at"])
    return df.sort_values("computed_at")  # oldest -> newest, for charting


st.title("📊 Real-Time Customer Intelligence Dashboard")
st.caption("Powered by a custom HyperLogLog + Count-Min Sketch streaming engine")

placeholder = st.empty()

while True:
    df = load_data()

    with placeholder.container():
        if df.empty:
            st.warning("No data yet — is the consumer running?")
        else:
            latest = df.iloc[-1]

            col1, col2 = st.columns(2)
            col1.metric("Unique Users (last 5 min, estimated)",
                        f"{latest['unique_users_estimate']:.0f}")
            col2.metric("Last Updated", latest["computed_at"].strftime("%H:%M:%S"))

            # --- Unique users over time ---
            st.subheader("Unique Users Trend")
            fig1 = px.line(df, x="computed_at", y="unique_users_estimate",
                            markers=True)
            st.plotly_chart(fig1, use_container_width=True)

            # --- Trending items (latest snapshot) ---
            st.subheader("Top Trending Items (latest snapshot)")
            top_items = latest["top_items"]
            if isinstance(top_items, str):
                top_items = json.loads(top_items)
            items_df = pd.DataFrame(top_items, columns=["item", "estimated_count"])
            fig2 = px.bar(items_df, x="item", y="estimated_count")
            st.plotly_chart(fig2, use_container_width=True)

            # --- Raw table ---
            with st.expander("Raw snapshot data"):
                display_df = df[::-1].copy()
                display_df["top_items"] = display_df["top_items"].apply(
                    lambda x: json.dumps(x) if not isinstance(x, str) else x
                )
                st.dataframe(display_df, use_container_width=True)

    time.sleep(10)  # refresh every 10s, matching your snapshot interval