"""Validation of incoming Kraken payloads. Rejects are returned with a reason,
never silently dropped (callers persist them to rejected_records)."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

MAX_FUTURE_SKEW = timedelta(seconds=5)


def _positive_decimal(raw, name):
    try:
        value = Decimal(str(raw))
    except InvalidOperation:
        return None, f'{name} is not numeric: {raw!r}'
    if not value.is_finite() or value <= 0:
        return None, f'{name} must be a positive finite number: {raw!r}'
    return value, None


def validate_trade(symbol: str, row, now=None):
    """Return (record, None) when valid, else (None, reason)."""
    now = now or datetime.now(timezone.utc)
    if not isinstance(row, (list, tuple)) or len(row) < 7:
        return None, 'row has fewer than 7 fields (trade_id missing?)'

    price, err = _positive_decimal(row[0], 'price')
    if err:
        return None, err
    volume, err = _positive_decimal(row[1], 'volume')
    if err:
        return None, err

    try:
        ts = datetime.fromtimestamp(float(row[2]), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None, f'invalid timestamp: {row[2]!r}'
    if ts > now + MAX_FUTURE_SKEW:
        return None, f'timestamp in the future: {ts.isoformat()}'

    side, order_type = row[3], row[4]
    if side not in ('b', 's'):
        return None, f'invalid side: {side!r}'
    if order_type not in ('m', 'l'):
        return None, f'invalid order type: {order_type!r}'

    try:
        trade_id = int(row[6])
    except (TypeError, ValueError):
        return None, f'invalid trade_id: {row[6]!r}'

    return {
        'symbol': symbol,
        'trade_id': trade_id,
        'price': price,
        'volume': volume,
        'side': side,
        'order_type': order_type,
        'trade_ts': ts,
    }, None


def validate_ticker(symbol: str, data: dict):
    """Ticker payload: a=ask[price,..], b=bid[price,..], c=last trade[price,..]."""
    try:
        ask, bid, last = data['a'][0], data['b'][0], data['c'][0]
    except (KeyError, IndexError, TypeError):
        return None, 'ticker payload missing a/b/c fields'
    out = {'symbol': symbol}
    for key, raw in (('ask', ask), ('bid', bid), ('last', last)):
        value, err = _positive_decimal(raw, key)
        if err:
            return None, err
        out[key] = value
    return out, None
