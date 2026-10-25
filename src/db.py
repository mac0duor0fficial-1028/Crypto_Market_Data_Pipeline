"""PostgreSQL helpers. All writes go through `transaction()` so that inserts and
the ingestion cursor are committed (or rolled back) together."""
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values


@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=int(os.environ.get('POSTGRES_PORT', '5432')),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        connect_timeout=10,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def transaction(dict_rows: bool = False):
    with get_conn() as conn:
        factory = RealDictCursor if dict_rows else None
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur


# ---- symbols / cursor ------------------------------------------------------
def symbol_for_pair(cur, kraken_pair):
    cur.execute('SELECT symbol FROM symbols WHERE kraken_pair = %s', (kraken_pair,))
    row = cur.fetchone()
    return row[0] if row else None


def upsert_symbol(cur, kraken_pair, symbol, base, quote):
    cur.execute(
        """INSERT INTO symbols (kraken_pair, symbol, base, quote)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (kraken_pair) DO UPDATE SET is_active = TRUE""",
        (kraken_pair, symbol, base, quote))


def get_cursor_value(cur, symbol):
    cur.execute('SELECT since_cursor FROM ingestion_state WHERE symbol = %s', (symbol,))
    row = cur.fetchone()
    return row[0] if row else None


def set_cursor_value(cur, symbol, since):
    cur.execute(
        """INSERT INTO ingestion_state (symbol, since_cursor, updated_at)
           VALUES (%s, %s, now())
           ON CONFLICT (symbol) DO UPDATE
           SET since_cursor = EXCLUDED.since_cursor, updated_at = now()""",
        (symbol, since))


# ---- inserts -----------------------------------------------------------------
def insert_trades(cur, records) -> int:
    """Insert validated trades; duplicates are ignored. Returns rows actually inserted."""
    if not records:
        return 0
    rows = [(r['symbol'], r['trade_id'], r['price'], r['volume'],
             r['side'], r['order_type'], r['trade_ts']) for r in records]
    inserted = execute_values(
        cur,
        """INSERT INTO trades (symbol, trade_id, price, volume, side, order_type, trade_ts)
           VALUES %s ON CONFLICT (symbol, trade_id) DO NOTHING RETURNING 1""",
        rows, page_size=1000, fetch=True)
    return len(inserted)


def insert_rejects(cur, source, items):
    """items: iterable of (payload, reason)."""
    items = list(items)
    if not items:
        return
    execute_values(
        cur,
        'INSERT INTO rejected_records (source, payload, reason) VALUES %s',
        [(source, Json(payload, dumps=_dumps), reason) for payload, reason in items])


def insert_price(cur, rec, captured_at):
    cur.execute(
        """INSERT INTO prices (symbol, bid, ask, last, captured_at)
           VALUES (%s, %s, %s, %s, %s) ON CONFLICT (symbol, captured_at) DO NOTHING""",
        (rec['symbol'], rec['bid'], rec['ask'], rec['last'], captured_at))


def _dumps(obj):
    import json
    return json.dumps(obj, default=str)
