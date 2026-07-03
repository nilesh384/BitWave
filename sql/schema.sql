CREATE TABLE IF NOT EXISTS fact_realtime_metrics (
    id SERIAL PRIMARY KEY,
    computed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    window_seconds INTEGER NOT NULL,
    unique_users_estimate NUMERIC NOT NULL,
    top_items JSONB NOT NULL
);