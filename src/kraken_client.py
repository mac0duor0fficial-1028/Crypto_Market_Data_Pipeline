"""Small, rate-limited client for Kraken's PUBLIC REST market-data endpoints.

No API key is needed for these endpoints. Pair names: REST accepts e.g. XBTUSD
(Kraken calls BTC "XBT"); responses are keyed by Kraken's internal name (XXBTZUSD).
"""
import logging
import os
import time
from typing import Optional

import requests

log = logging.getLogger(__name__)

_ASSET_ALIASES = {'XBT': 'BTC', 'XDG': 'DOGE'}
_RETRYABLE = ('Rate limit', 'Temporary lockout', 'Service:Unavailable',
              'Service:Busy', 'Too many')


class KrakenAPIError(RuntimeError):
    """Raised for non-retryable API errors or when retries are exhausted."""


def normalize_symbol(wsname: str) -> str:
    """'XBT/USD' -> 'BTC/USD' (the symbol format used everywhere downstream)."""
    base, sep, quote = wsname.partition('/')
    if not sep or not base or not quote:
        raise ValueError(f'Unexpected wsname format: {wsname!r}')
    return f'{_ASSET_ALIASES.get(base, base)}/{_ASSET_ALIASES.get(quote, quote)}'


class KrakenClient:
    def __init__(self, base_url: Optional[str] = None,
                 min_interval: Optional[float] = None,
                 max_retries: int = 5, timeout: float = 10.0,
                 session=None, sleep=time.sleep, clock=time.monotonic):
        self.base_url = (base_url or os.environ.get('KRAKEN_BASE_URL',
                                                    'https://api.kraken.com')).rstrip('/')
        if min_interval is None:
            min_interval = float(os.environ.get('KRAKEN_MIN_INTERVAL_SECONDS', '1.2'))
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = session or requests.Session()
        self._sleep = sleep
        self._clock = clock
        self._last_call = None

    # -- internals ---------------------------------------------------------
    def _throttle(self):
        if self._last_call is not None:
            wait = self.min_interval - (self._clock() - self._last_call)
            if wait > 0:
                self._sleep(wait)
        self._last_call = self._clock()

    def _backoff(self, attempt: int, rate_limited: bool = False):
        base = 5 if rate_limited else 1
        self._sleep(min(base * (2 ** attempt), 60))

    def _get(self, path: str, params: dict):
        url = self.base_url + path
        last_error = None
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                body = resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                log.warning('Kraken request failed (%s), attempt %d/%d',
                            exc, attempt + 1, self.max_retries)
                self._backoff(attempt)
                continue
            errors = body.get('error') or []
            if errors:
                if any(marker in e for e in errors for marker in _RETRYABLE):
                    last_error = KrakenAPIError(errors)
                    log.warning('Kraken retryable error %s, attempt %d/%d',
                                errors, attempt + 1, self.max_retries)
                    self._backoff(attempt, rate_limited=True)
                    continue
                raise KrakenAPIError(errors)
            return body['result']
        raise KrakenAPIError(f'Kraken request to {path} failed after '
                             f'{self.max_retries} attempts: {last_error}')

    # -- public endpoints --------------------------------------------------
    def trades(self, pair: str, since: Optional[str] = None, count: int = 1000):
        """Return (rows, last_cursor).

        Each row: [price, volume, time, side(b/s), type(m/l), misc, trade_id].
        With since=None Kraken returns the most recent trades (NOT the full history).
        """
        params = {'pair': pair, 'count': count}
        if since is not None:
            params['since'] = since
        result = self._get('/0/public/Trades', params)
        last = result.get('last')
        series = [v for k, v in result.items() if k != 'last']
        if last is None or len(series) != 1:
            raise KrakenAPIError(f'Unexpected Trades payload keys: {list(result)}')
        return series[0], str(last)

    def ticker(self, pair: str) -> dict:
        result = self._get('/0/public/Ticker', {'pair': pair})
        if len(result) != 1:
            raise KrakenAPIError(f'Unexpected Ticker payload keys: {list(result)}')
        return next(iter(result.values()))

    def asset_pairs(self, pairs) -> dict:
        """Return {kraken_internal_name: metadata} for the requested pairs."""
        return self._get('/0/public/AssetPairs', {'pair': ','.join(pairs)})
