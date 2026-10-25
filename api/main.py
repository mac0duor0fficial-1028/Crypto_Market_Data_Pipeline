"""Read-only query API over the Cassandra serving layer.

Symbols in URLs use a dash: BTC-USD -> BTC/USD. All times are UTC.
"""
import os
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title='Crypto Market Data API', version='1.0.0')
_session = None


def session():
    global _session
    if _session is None:
        cluster = Cluster([os.environ.get('CASSANDRA_HOST', 'cassandra')])
        _session = cluster.connect(os.environ.get('CASSANDRA_KEYSPACE', 'crypto'))
        _session.row_factory = dict_factory
    return _session


def _symbol(path_symbol: str) -> str:
    return path_symbol.replace('-', '/').upper()


def _utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _serialize(row: dict) -> dict:
    out = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.replace(tzinfo=timezone.utc).isoformat()
        elif hasattr(value, 'date') and callable(value.date):  # cassandra.util.Date
            out[key] = value.date().isoformat()
        else:
            out[key] = value
    return out


@app.get('/health')
def health():
    session().execute('SELECT release_version FROM system.local')
    return {'status': 'ok'}


@app.get('/prices/{symbol}/latest')
def latest_price(symbol: str):
    rows = list(session().execute(
        'SELECT symbol, price, trade_ts FROM latest_price WHERE symbol = %s', (_symbol(symbol),)))
    if not rows:
        raise HTTPException(404, 'No price for symbol')
    return _serialize(rows[0])


@app.get('/aggregates/{symbol}')
def aggregates(symbol: str,
               start: datetime = Query(..., description='ISO-8601, UTC if no offset'),
               end: datetime = Query(...),
               interval: Literal['1m', '1h'] = '1m'):
    sym, start, end = _symbol(symbol), _utc_naive(start), _utc_naive(end)
    if end <= start:
        raise HTTPException(400, 'end must be after start')
    cols = ('window_start, avg_price, min_price, max_price, vwap, volume, '
            'trade_count, volatility')
    results = []
    if interval == '1m':
        if end - start > timedelta(days=7):
            raise HTTPException(400, '1m queries are limited to 7 days')
        day = start.date()
        while day <= end.date():
            results += list(session().execute(
                f'SELECT {cols} FROM trade_agg_1m WHERE symbol=%s AND day=%s '
                'AND window_start >= %s AND window_start < %s', (sym, day, start, end)))
            day += timedelta(days=1)
    else:
        if end - start > timedelta(days=92):
            raise HTTPException(400, '1h queries are limited to 92 days')
        year, month = start.year, start.month
        while (year, month) <= (end.year, end.month):
            results += list(session().execute(
                f'SELECT {cols} FROM trade_agg_1h WHERE symbol=%s AND month=%s '
                'AND window_start >= %s AND window_start < %s',
                (sym, f'{year:04d}-{month:02d}', start, end)))
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    results.sort(key=lambda r: r['window_start'])
    return [_serialize(r) for r in results]


@app.get('/anomalies/{symbol}')
def anomalies(symbol: str, day: date = Query(None, description='UTC day, default today')):
    day = day or datetime.now(timezone.utc).date()
    rows = session().execute(
        'SELECT window_start, kind, details FROM anomalies WHERE symbol=%s AND day=%s',
        (_symbol(symbol), day))
    return [_serialize(r) for r in rows]
