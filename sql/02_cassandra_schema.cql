-- Serving schema. Tables are modelled per dashboard/API query.
-- Primary keys include window_start, so re-writing a window is an idempotent upsert.

CREATE KEYSPACE IF NOT EXISTS crypto
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};  -- dev only

-- Q1: per-minute metrics for a symbol on a given (UTC) day
CREATE TABLE IF NOT EXISTS crypto.trade_agg_1m (
    symbol       text,
    day          date,
    window_start timestamp,
    avg_price    double,
    min_price    double,
    max_price    double,
    vwap         double,
    volume       double,
    trade_count  bigint,
    volatility   double,
    PRIMARY KEY ((symbol, day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Q2: hourly rollup for a symbol in a given month ('YYYY-MM')
CREATE TABLE IF NOT EXISTS crypto.trade_agg_1h (
    symbol       text,
    month        text,
    window_start timestamp,
    avg_price    double,
    min_price    double,
    max_price    double,
    vwap         double,
    volume       double,
    trade_count  bigint,
    volatility   double,
    PRIMARY KEY ((symbol, month), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Q3: latest price per symbol
CREATE TABLE IF NOT EXISTS crypto.latest_price (
    symbol    text PRIMARY KEY,
    price     double,
    trade_ts  timestamp
);

-- Q4: anomalies for a symbol on a given day
CREATE TABLE IF NOT EXISTS crypto.anomalies (
    symbol        text,
    day           date,
    window_start  timestamp,
    kind          text,
    details       text,
    PRIMARY KEY ((symbol, day), window_start, kind)
) WITH CLUSTERING ORDER BY (window_start DESC, kind ASC);
