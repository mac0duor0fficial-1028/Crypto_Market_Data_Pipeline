import pytest
import requests

from src.kraken_client import KrakenAPIError, KrakenClient, normalize_symbol


class FakeResp:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'{self.status_code}')

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def client(responses, **kw):
    return KrakenClient(base_url='https://api.test', min_interval=0, session=FakeSession(responses),
                        sleep=lambda s: None, **kw)


def test_normalize_symbol():
    assert normalize_symbol('XBT/USD') == 'BTC/USD'
    assert normalize_symbol('ETH/USD') == 'ETH/USD'
    assert normalize_symbol('XDG/USD') == 'DOGE/USD'
    with pytest.raises(ValueError):
        normalize_symbol('BTCUSD')


def test_trades_parses_internal_key_and_cursor():
    body = {'error': [], 'result': {'XXBTZUSD': [['1', '2', 3.0, 'b', 'm', '', 9]],
                                    'last': '1700000000000000000'}}
    c = client([FakeResp(body)])
    rows, last = c.trades('XBTUSD')
    assert rows == [['1', '2', 3.0, 'b', 'm', '', 9]] and last == '1700000000000000000'
    _, params = c.session.calls[0]
    assert 'since' not in params and params['pair'] == 'XBTUSD'


def test_since_is_sent_when_given():
    body = {'error': [], 'result': {'XXBTZUSD': [], 'last': '5'}}
    c = client([FakeResp(body)])
    c.trades('XBTUSD', since='4')
    assert c.session.calls[0][1]['since'] == '4'


def test_retries_rate_limit_then_succeeds():
    ok = {'error': [], 'result': {'XXBTZUSD': [], 'last': '1'}}
    c = client([FakeResp({'error': ['EAPI:Rate limit exceeded'], 'result': {}}), FakeResp(ok)])
    assert c.trades('XBTUSD') == ([], '1')
    assert len(c.session.calls) == 2


def test_retries_network_error():
    ok = {'error': [], 'result': {'XXBTZUSD': [], 'last': '1'}}
    c = client([requests.ConnectionError('boom'), FakeResp(ok)])
    assert c.trades('XBTUSD')[1] == '1'


def test_non_retryable_error_raises_immediately():
    c = client([FakeResp({'error': ['EQuery:Unknown asset pair'], 'result': {}})])
    with pytest.raises(KrakenAPIError):
        c.trades('NOPE')
    assert len(c.session.calls) == 1


def test_gives_up_after_max_retries():
    c = client([requests.ConnectionError('x')] * 3, max_retries=3)
    with pytest.raises(KrakenAPIError):
        c.trades('XBTUSD')


def test_throttle_spaces_calls():
    sleeps, t = [], [0.0]
    c = KrakenClient(base_url='https://x', min_interval=1.0, sleep=sleeps.append,
                     clock=lambda: t[0],
                     session=FakeSession([FakeResp({'error': [], 'result': {'K': {}}})] * 2))
    c.ticker('XBTUSD'); c.ticker('XBTUSD')
    assert sleeps == [1.0]
