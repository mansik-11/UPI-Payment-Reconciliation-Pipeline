-- =============================================================================
-- UPI Payment Reconciliation Pipeline - Database Initialization
-- Schemas: app, raw, staging, marts
-- =============================================================================

-- 1. Create schemas
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- =============================================================================
-- 2. Schema: app (Source App OLTP Tables)
-- =============================================================================

CREATE TABLE IF NOT EXISTS app.merchants (
    merchant_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(64) NOT NULL,
    fee_rate NUMERIC(6, 4) NOT NULL, -- e.g. 0.0150 for 1.5%
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS app.orders (
    order_id VARCHAR(64) PRIMARY KEY,
    merchant_id VARCHAR(64) NOT NULL REFERENCES app.merchants(merchant_id),
    customer_vpa VARCHAR(255) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(32) NOT NULL, -- created, paid, failed, refunded
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_orders_updated_at ON app.orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_app_orders_merchant_id ON app.orders(merchant_id);
CREATE INDEX IF NOT EXISTS idx_app_merchants_updated_at ON app.merchants(updated_at);

-- =============================================================================
-- 3. Schema: raw (Bronze Ingestion Layer)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id VARCHAR(64),
    merchant_id VARCHAR(64),
    customer_vpa VARCHAR(255),
    amount NUMERIC(12, 2),
    status VARCHAR(32),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    run_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_orders_run_date ON raw.orders(run_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id ON raw.orders(order_id);

CREATE TABLE IF NOT EXISTS raw.gateway_events (
    txn_id VARCHAR(64),
    order_id VARCHAR(64),
    gateway_status VARCHAR(32),
    amount NUMERIC(12, 2),
    event_time TIMESTAMPTZ,
    event_type VARCHAR(32),
    run_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_gateway_run_date ON raw.gateway_events(run_date);
CREATE INDEX IF NOT EXISTS idx_raw_gateway_txn_id ON raw.gateway_events(txn_id);
CREATE INDEX IF NOT EXISTS idx_raw_gateway_order_id ON raw.gateway_events(order_id);

CREATE TABLE IF NOT EXISTS raw.settlements (
    settlement_id VARCHAR(64),
    txn_id VARCHAR(64),
    gross_amount NUMERIC(12, 2),
    mdr_fee NUMERIC(12, 2),
    gst_on_fee NUMERIC(12, 2),
    net_settled NUMERIC(12, 2),
    settled_date DATE,
    settlement_type VARCHAR(16) DEFAULT 'payment', -- 'payment' or 'refund' (refund rows carry negative amounts)
    run_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_settlements_run_date ON raw.settlements(run_date);
CREATE INDEX IF NOT EXISTS idx_raw_settlements_txn_id ON raw.settlements(txn_id);

-- Dead-letter table for rejected invalid records
CREATE TABLE IF NOT EXISTS raw.rejected (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL, -- 'orders', 'gateway', 'settlement'
    row_data JSONB NOT NULL,
    reason TEXT NOT NULL,
    run_date DATE NOT NULL,
    rejected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_rejected_run_date ON raw.rejected(run_date);
CREATE INDEX IF NOT EXISTS idx_raw_rejected_source ON raw.rejected(source);

-- Pipeline execution audit and metrics log
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    step VARCHAR(64) NOT NULL,
    rows_in INTEGER DEFAULT 0,
    rows_out INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    duration_seconds NUMERIC(8, 2) DEFAULT 0.0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_pipeline_runs_run_date ON raw.pipeline_runs(run_date);
