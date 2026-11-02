def validate_trade(row):
    """Return a clean dict, or raise ValueError so it goes to the reject log."""
    try:
        price, volume, ts, side, ordtype, misc, trade_id = row[:7]
        price, volume, ts = float(price), float(volume), float(ts)
    except (ValueError, TypeError, IndexError) as e:
        raise ValueError(f"malformed trade: {row!r}") from e
    if price <= 0 or volume <= 0:
        raise ValueError(f"non-positive price/volume: {row!r}")
    if side not in ("b", "s"):
        raise ValueError(f"bad side: {row!r}")
    return {
        "trade_id": int(trade_id),
        "price": price,
        "volume": volume,
        "trade_ts": ts,
        "side": "buy" if side == "b" else "sell",
        "order_type": "market" if ordtype == "m" else "limit",
    }