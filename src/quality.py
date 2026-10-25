"""Data-quality checks used by the Airflow DAG. Each raises DataQualityError on breach."""
import logging

from src.config import env
from src.db import transaction

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


def check_freshness(max_age_minutes=None):
    limit = float(max_age_minutes or env('QUALITY_MAX_STALENESS_MIN', '5'))
    with transaction() as cur:
        cur.execute('SELECT max(ingested_at), now() FROM trades')
        last, now = cur.fetchone()
    if last is None:
        log.warning('No trades ingested yet; skipping freshness check')
        return None
    age = (now - last).total_seconds() / 60
    if age > limit:
        raise DataQualityError(f'No new trades for {age:.1f} min (limit {limit} min)')
    return age


def check_backlog(max_unpublished=None):
    limit = int(max_unpublished or env('QUALITY_MAX_BACKLOG', '50000'))
    with transaction() as cur:
        cur.execute('SELECT count(*) FROM trades WHERE published_at IS NULL')
        backlog = cur.fetchone()[0]
    if backlog > limit:
        raise DataQualityError(f'Unpublished backlog {backlog} exceeds limit {limit}')
    return backlog


def check_reject_rate(window_minutes=60, max_rate=None):
    limit = float(max_rate or env('QUALITY_MAX_REJECT_RATE', '0.01'))
    with transaction() as cur:
        cur.execute(
            """SELECT
                 (SELECT count(*) FROM rejected_records
                   WHERE source LIKE 'kraken.%%'
                     AND created_at > now() - make_interval(mins => %s)),
                 (SELECT count(*) FROM trades
                   WHERE ingested_at > now() - make_interval(mins => %s))""",
            (window_minutes, window_minutes))
        rejected, accepted = cur.fetchone()
    total = rejected + accepted
    rate = rejected / total if total else 0.0
    if rate > limit:
        raise DataQualityError(f'Reject rate {rate:.2%} exceeds {limit:.2%}')
    return rate
