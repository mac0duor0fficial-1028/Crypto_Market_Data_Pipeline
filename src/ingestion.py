"""Ingestion logic called by the Airflow DAG (kept out of the DAG file so it is testable)."""
import logging
from datetime import datetime, timezone

from src.config import env
from src.db import (get_cursor_value, insert_price, insert_rejects, insert_trades,
                    set_cursor_value, symbol_for_pair, transaction, upsert_symbol)
from src.kraken_client import KrakenClient, normalize_symbol
from src.validators import validate_ticker, validate_trade

log = logging.getLogger(__name__)
PAGE_SIZE = 1000


def ensure_symbols(pairs, client=None) -> dict:
    """Seed the symbols table for any configured pair not yet present."""
    with transaction() as cur:
        missing = [p for p in pairs if symbol_for_pair(cur, p) is None]
    if missing:
        client = client or KrakenClient()
        meta_by_name = client.asset_pairs(missing)
        with transaction() as cur:
            for pair in missing:
                meta = next((m for name, m in meta_by_name.items()
                             if pair in (name, m.get('altname'))), None)
                if meta is None or not meta.get('wsname'):
                    raise ValueError(f'Kraken returned no usable metadata for pair {pair}')
                symbol = normalize_symbol(meta['wsname'])
                base, quote = symbol.split('/')
                upsert_symbol(cur, pair, symbol, base, quote)
                log.info('Registered %s -> %s', pair, symbol)
    with transaction() as cur:
        return {p: symbol_for_pair(cur, p) for p in pairs}


def ingest_trades(pair, client=None) -> dict:
    """Pull new trades for one pair. Each page commits trades + rejects + cursor atomically."""
    client = client or KrakenClient()
    max_pages = int(env('KRAKEN_MAX_PAGES_PER_RUN', '5'))
    with transaction() as cur:
        symbol = symbol_for_pair(cur, pair)
        if symbol is None:
            raise RuntimeError(f'Pair {pair} is not registered; run ensure_symbols first')
        since = get_cursor_value(cur, symbol)

    stats = {'pair': pair, 'symbol': symbol, 'fetched': 0, 'inserted': 0, 'rejected': 0}
    for _ in range(max_pages):
        rows, last = client.trades(pair, since=since, count=PAGE_SIZE)
        good, bad = [], []
        for row in rows:
            record, reason = validate_trade(symbol, row)
            if record is not None:
                good.append(record)
            else:
                bad.append((row, reason))
        with transaction() as cur:
            stats['inserted'] += insert_trades(cur, good)
            insert_rejects(cur, f'kraken.trades.{pair}', bad)
            set_cursor_value(cur, symbol, last)
        stats['fetched'] += len(rows)
        stats['rejected'] += len(bad)
        since = last
        if len(rows) < PAGE_SIZE:
            break
    log.info('Ingested %s', stats)
    return stats


def ingest_ticker(pair, client=None) -> dict:
    client = client or KrakenClient()
    with transaction() as cur:
        symbol = symbol_for_pair(cur, pair)
    if symbol is None:
        raise RuntimeError(f'Pair {pair} is not registered')
    record, reason = validate_ticker(symbol, client.ticker(pair))
    if record is None:
        with transaction() as cur:
            insert_rejects(cur, f'kraken.ticker.{pair}', [({'pair': pair}, reason)])
        return {'pair': pair, 'stored': False, 'reason': reason}
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    with transaction() as cur:
        insert_price(cur, record, captured_at)
    return {'pair': pair, 'stored': True}
