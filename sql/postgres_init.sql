CREATE TABLE IF NOT EXISTS symbols (
    symbol      TEXT PRIMARY KEY,          -- e.g. XBTUSD
    base_asset  TEXT,
    quote_asset TEXT,
    added_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trades (
    symbol       TEXT        NOT NULL REFERENCES symbols(symbol),
    trade_id     BIGINT      NOT NULL,
    price        NUMERIC(18,8) NOT NULL,
    volume       NUMERIC(18,8) NOT NULL,
    side         TEXT        NOT NULL,
    order_type   TEXT,
    trade_ts     TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published    BOOLEAN     NOT NULL DEFAULT FALSE,   -- outbox flag
    PRIMARY KEY (symbol, trade_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_unpublished
    ON trades (trade_ts) WHERE published = FALSE;

-- Periodic ticker snapshots
CREATE TABLE IF NOT EXISTS prices (
    symbol      TEXT NOT NULL REFERENCES symbols(symbol),
    captured_at TIMESTAMPTZ NOT NULL,
    last_price  NUMERIC(18,8),
    bid         NUMERIC(18,8),
    ask         NUMERIC(18,8),
    volume_24h  NUMERIC(24,8),
    PRIMARY KEY (symbol, captured_at)
);

-- Ingestion cursor per symbol
CREATE TABLE IF NOT EXISTS ingestion_state (
    symbol     TEXT PRIMARY KEY,
    since      TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Records that failed validation (basic dead-letter store)
CREATE TABLE IF NOT EXISTS rejected_records (
    id          BIGSERIAL PRIMARY KEY,
    symbol      TEXT,
    reason      TEXT,
    rejected_at TIMESTAMPTZ DEFAULT now()
);