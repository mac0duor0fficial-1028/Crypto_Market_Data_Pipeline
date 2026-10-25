"""Compare PostgreSQL (source of truth) with Cassandra 1-minute aggregates.

Used for the restart-proof evidence: after a controlled Spark/Kafka/publisher restart,
every CLOSED minute must have identical trade_count (and volume) in both stores, i.e.
no lost and no duplicated analytical results.

Only minutes older than --lag-minutes are compared, so windows still open (watermark)
are excluded. Exit code 1 on any mismatch.
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from cassandra.cluster import Cluster


def floor_minute(dt):
    return dt.replace(second=0, microsecond=0)


def postgres_counts(symbol, start, end):
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=int(os.environ.get('POSTGRES_PORT', '5432')),
        dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'])
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT date_trunc('minute', trade_ts AT TIME ZONE 'UTC'), count(*), sum(volume)
                     FROM trades
                    WHERE symbol = %s AND trade_ts >= %s AND trade_ts < %s
                      AND published_at IS NOT NULL
                    GROUP BY 1""", (symbol, start, end))
            return {m: (int(c), float(v)) for m, c, v in cur.fetchall()}
    finally:
        conn.close()


def cassandra_counts(symbol, start, end):
    session = Cluster([os.environ.get('CASSANDRA_HOST', 'cassandra')]).connect(
        os.environ.get('CASSANDRA_KEYSPACE', 'crypto'))
    naive_start, naive_end = start.replace(tzinfo=None), end.replace(tzinfo=None)
    out, day = {}, naive_start.date()
    while day <= naive_end.date():
        for row in session.execute(
                'SELECT window_start, trade_count, volume FROM trade_agg_1m '
                'WHERE symbol=%s AND day=%s AND window_start >= %s AND window_start < %s',
                (symbol, day, naive_start, naive_end)):
            out[row.window_start] = (int(row.trade_count), float(row.volume))
        day += timedelta(days=1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--symbol', default='BTC/USD')
    ap.add_argument('--minutes', type=int, default=60, help='size of the range to compare')
    ap.add_argument('--lag-minutes', type=int, default=15, help='skip the newest N minutes')
    args = ap.parse_args()

    end = floor_minute(datetime.now(timezone.utc)) - timedelta(minutes=args.lag_minutes)
    start = end - timedelta(minutes=args.minutes)
    pg = postgres_counts(args.symbol, start, end)
    cs = cassandra_counts(args.symbol, start, end)

    missing = sorted(set(pg) - set(cs))
    extra = sorted(set(cs) - set(pg))
    mismatched = [m for m in sorted(set(pg) & set(cs))
                  if pg[m][0] != cs[m][0]
                  or abs(pg[m][1] - cs[m][1]) > 1e-6 * max(1.0, pg[m][1])]

    print(f'Symbol {args.symbol}  range {start:%Y-%m-%d %H:%M} -> {end:%Y-%m-%d %H:%M} UTC')
    print(f'Postgres minutes: {len(pg)}  Cassandra minutes: {len(cs)}')
    print(f'Total trades  PG={sum(c for c, _ in pg.values())}  '
          f'CS={sum(c for c, _ in cs.values())}')
    print(f'Missing in Cassandra: {len(missing)}  Extra in Cassandra: {len(extra)}  '
          f'Mismatched: {len(mismatched)}')
    for m in missing[:10]:
        print('  missing   ', m, pg[m])
    for m in extra[:10]:
        print('  extra     ', m, cs[m])
    for m in mismatched[:10]:
        print('  mismatch  ', m, 'PG', pg[m], 'CS', cs[m])
    ok = not (missing or extra or mismatched)
    print('RESULT:', 'PASS - no lost or duplicated results' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
