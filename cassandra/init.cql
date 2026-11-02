CREATE KEYSPACE IF NOT EXISTS crypto
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto;

CREATE TABLE IF NOT EXISTS trade_stats_1m (
    symbol       text,
    day          date,
    window_start timestamp,
    window_end   timestamp,
    avg_price    double,
    min_price    double,
    max_price    double,
    volatility   double,
    vwap         double,
    total_volume double,
    buy_volume   double,
    trade_count  bigint,
    PRIMARY KEY ((symbol, day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);