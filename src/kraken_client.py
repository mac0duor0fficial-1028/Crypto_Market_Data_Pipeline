import time
import requests

BASE = "https://api.kraken.com/0/public"

class KrakenError(Exception):
    pass

class KrakenClient:
    def __init__(self, min_interval=1.0, timeout=10, retries=3):
        self.min_interval = min_interval
        self.timeout = timeout
        self.retries = retries
        self._last_call = 0.0
        self.session = requests.Session()

    def _get(self, path, params=None):
        for attempt in range(self.retries):
            wait = self.min_interval - (time.time() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()
            try:
                r = self.session.get(f"{BASE}/{path}", params=params, timeout=self.timeout)
                r.raise_for_status()
                body = r.json()
                if body.get("error"):
                    raise KrakenError(body["error"])
                return body["result"]
            except (requests.RequestException, KrakenError):
                if attempt == self.retries - 1:
                    raise
                time.sleep(2 ** attempt)  # exponential backoff

    def asset_pairs(self):
        return self._get("AssetPairs")

    def ticker(self, pair):
        return self._get("Ticker", {"pair": pair})

    def trades(self, pair, since=None):
        params = {"pair": pair}
        if since:
            params["since"] = since
        result = self._get("Trades", params)
        last = result.pop("last")
        (_, rows), = result.items()   # one pair per call
        return rows, last

    def ohlc(self, pair, interval=1, since=None):
        params = {"pair": pair, "interval": interval}
        if since:
            params["since"] = since
        return self._get("OHLC", params)