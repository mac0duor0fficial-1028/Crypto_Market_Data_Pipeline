import json, os
from confluent_kafka import Producer
from src import db

def publish_unpublished(batch_size=5000):
    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "enable.idempotence": True,
        "acks": "all",
    })
    topic = os.environ["KAFKA_TOPIC_TRADES"]
    with db.connect() as conn:
        rows = db.fetch_unpublished(conn, batch_size)  # ordered by trade_ts
        sent = []
        def on_delivery(err, msg):
            if err is None:
                sent.append(msg.key().decode() + ":" + json.loads(msg.value())["trade_id"].__str__())
        for r in rows:
            event = {
                "schema_version": 1, "symbol": r["symbol"],
                "trade_id": r["trade_id"], "price": float(r["price"]),
                "volume": float(r["volume"]), "side": r["side"],
                "trade_time": r["trade_ts"].timestamp(), "source": "kraken",
            }
            producer.produce(topic, key=r["symbol"], value=json.dumps(event),
                             on_delivery=on_delivery)
        producer.flush(30)
        # mark only confirmed rows as published
        confirmed = {(r["symbol"], r["trade_id"]) for r in rows
                     if f"{r['symbol']}:{r['trade_id']}" in sent}
        db.mark_published(conn, confirmed)
    return len(confirmed)