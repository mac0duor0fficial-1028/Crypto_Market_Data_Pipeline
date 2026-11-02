from psycopg2.extras import execute_values

def insert_trades(conn, symbol, trades):
    if not trades:
        return
    with conn.cursor() as cur:
        cur.execute("INSERT INTO symbols(symbol) VALUES (%s) ON CONFLICT DO NOTHING", (symbol,))
        execute_values(cur, """
            INSERT INTO trades (symbol, trade_id, price, volume, side, order_type, trade_ts)
            VALUES %s ON CONFLICT (symbol, trade_id) DO NOTHING
        """, [(symbol, t["trade_id"], t["price"], t["volume"], t["side"],
               t["order_type"], f"to_timestamp({t['trade_ts']})") for t in trades],
        template="(%s,%s,%s,%s,%s,%s, to_timestamp(%s))".replace("to_timestamp(%s))", "to_timestamp(%s))"))