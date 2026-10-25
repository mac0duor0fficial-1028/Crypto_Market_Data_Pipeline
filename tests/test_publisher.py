from datetime import datetime, timezone
from decimal import Decimal

from src.publisher import to_event


def test_to_event_matches_schema_fields():
    import json
    from pathlib import Path
    schema = json.loads((Path(__file__).parents[1] / 'kafka/schemas/trade_event.avsc').read_text())
    row = {'id': 1, 'symbol': 'BTC/USD', 'trade_id': 77, 'price': Decimal('67000.10'),
           'volume': Decimal('0.015'), 'side': 'b', 'order_type': 'm',
           'trade_ts': datetime(2026, 10, 25, 12, 0, 0, 123000, tzinfo=timezone.utc)}
    ev = to_event(row)
    assert ev['event_id'] == 'BTC/USD:77'
    assert ev['price'] == 67000.10 and isinstance(ev['volume'], float)
    assert ev['trade_ts_ms'] == int(row['trade_ts'].timestamp() * 1000)
    assert set(ev) == {f['name'] for f in schema['fields']}
