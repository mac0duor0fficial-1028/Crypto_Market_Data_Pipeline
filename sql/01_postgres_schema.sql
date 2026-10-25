-- Staging schema (runs inside POSTGRES_DB on first start).

CREATE TABLE IF NOT EXISTS symbols (
    symbol_id    SERIAL PRIMARY KEY,
    kraken_pair  TEXT UNIQUE NOT NULL,          -- as accepted by the API, e.g. XBTUSD
    symbol       TEXT UNIQUE NOT NULL,          -- normalized, e.g. BTC/USD
    base         TEXT NOT NULL,
    quote        TEXT NOT NULL,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trades (
    id            BIGSERIAL PRIMARY KEY,
    symbol        TEXT NOT NULL REFERENCES symbols(symbol),
    trade_id      BIGINT NOT NULL,
    price         NUMERIC(28,10) NOT NULL CHECK (price > 0),
    volume        NUMERIC(28,10) NOT NULL CHECK (volume > 0),
    side          CHAR(1) NOT NULL CHECK (side IN ('b','s')),
    order_type    CHAR(1) NOT NULL CHECK (order_type IN ('m','l')),
    trade_ts      TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,                  -- NULL = not yet sent to Kafka
    UNIQUE (symbol, trade_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_unpublished ON trades (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts   ON trades (symbol, trade_ts);

CREATE TABLE IF NOT EXISTS prices (             -- ticker snapshots
    id           BIGSERIAL PRIMARY KEY,
    symbol       TEXT NOT NULL REFERENCES symbols(symbol),
    bid          NUMERIC(28,10),
    ask          NUMERIC(28,10),
    last         NUMERIC(28,10),
    captured_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (symbol, captured_at)
);

CREATE TABLE IF NOT EXISTS ingestion_state (    -- Kraken "last" cursor per symbol
    symbol        TEXT PRIMARY KEY REFERENCES symbols(symbol),
    since_cursor  TEXT,                         -- NULL until first successful run
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_records (   -- dead letters (bad data / unserializable events)
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    payload     JSONB,
    reason      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
