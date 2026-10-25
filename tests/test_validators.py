from datetime import datetime, timezone

from src.validators import validate_ticker, validate_trade

NOW = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
TS = NOW.timestamp() - 10


def good_row(**over):
    row = ['67000.10', '0.0150', TS, 'b', 'm', '', 123456]
    for idx, val in over.items():
        row[int(idx[1:])] = val
    return row


def test_valid_trade():
    rec, err = validate_trade('BTC/USD', good_row(), now=NOW)
    assert err is None
    assert rec['symbol'] == 'BTC/USD' and rec['trade_id'] == 123456
    assert str(rec['price']) == '67000.10' and rec['side'] == 'b'
    assert rec['trade_ts'].tzinfo is not None


def test_rejects_short_row():
    assert validate_trade('BTC/USD', ['1', '1', TS, 'b', 'm', ''], now=NOW)[0] is None


def test_rejects_bad_price_volume():
    for bad in ('0', '-1', 'abc', 'NaN', 'Infinity'):
        assert validate_trade('X', good_row(f0=bad), now=NOW)[0] is None
        assert validate_trade('X', good_row(f1=bad), now=NOW)[0] is None


def test_rejects_bad_side_and_type():
    assert validate_trade('X', good_row(f3='x'), now=NOW)[0] is None
    assert validate_trade('X', good_row(f4='z'), now=NOW)[0] is None


def test_rejects_future_timestamp():
    _, err = validate_trade('X', good_row(f2=NOW.timestamp() + 60), now=NOW)
    assert 'future' in err


def test_rejects_bad_trade_id():
    assert validate_trade('X', good_row(f6='abc'), now=NOW)[0] is None


def test_ticker():
    rec, err = validate_ticker('BTC/USD', {'a': ['67001.0', '1', '1.0'],
                                          'b': ['67000.0', '1', '1.0'],
                                          'c': ['67000.5', '0.1']})
    assert err is None and str(rec['last']) == '67000.5'
    assert validate_ticker('BTC/USD', {'a': ['x']})[0] is None
