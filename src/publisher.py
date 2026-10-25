"""PostgreSQL -> Kafka publisher (poll-and-mark outbox pattern).

Guarantee: at-least-once. A row is marked published only after Kafka acknowledged it.
If the process dies between acknowledgement and the UPDATE, the row is re-sent; the
Spark job de-duplicates on event_id and Cassandra writes are upserts.

Heavy imports (confluent_kafka, db) are lazy so to_event() is unit-testable anywhere.
"""
import json
import logging
import os
from functools import partial
from pathlib import Path

log = logging.getLogger(__name__)

SELECT_UNPUBLISHED = """
    SELECT id, symbol, trade_id, price, volume, side, order_type, trade_ts
      FROM trades
     WHERE published_at IS NULL
     ORDER BY id
     LIMIT %s
       FOR UPDATE SKIP LOCKED
"""


def to_event(row) -> dict:
    """Map a trades row to the TradeEvent Avro record (see kafka/schemas/trade_event.avsc)."""
    return {
        'event_id': f"{row['symbol']}:{row['trade_id']}",
        'symbol': row['symbol'],
        'trade_id': int(row['trade_id']),
        'price': float(row['price']),
        'volume': float(row['volume']),
        'side': row['side'],
        'order_type': row['order_type'],
        'trade_ts_ms': int(round(row['trade_ts'].timestamp() * 1000)),
        'exchange': 'kraken',
        'schema_version': 1,
    }


def _schema_path() -> Path:
    override = os.environ.get('AVRO_SCHEMA_PATH')
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / 'kafka' / 'schemas' / 'trade_event.avsc'


def build_producers():
    from confluent_kafka import Producer, SerializingProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import StringSerializer

    bootstrap = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
    registry = SchemaRegistryClient({'url': os.environ.get('SCHEMA_REGISTRY_URL',
                                                           'http://schema-registry:8081')})
    serializer = AvroSerializer(registry, _schema_path().read_text())
    producer = SerializingProducer({
        'bootstrap.servers': bootstrap,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': serializer,
        'enable.idempotence': True,
        'acks': 'all',
        'linger.ms': 50,
        'delivery.timeout.ms': 30000,
    })
    dlq = Producer({'bootstrap.servers': bootstrap})
    return producer, dlq


def _on_delivery(row_id, delivered, failed, err, msg):
    if err is not None:
        failed[row_id] = str(err)
    else:
        delivered.add(row_id)


def publish_batch(producer, dlq, topic, dlq_topic, batch_size) -> dict:
    from confluent_kafka.serialization import SerializationError
    from src.db import insert_rejects, transaction

    delivered, failed, dead = set(), {}, set()
    with transaction(dict_rows=True) as cur:
        cur.execute(SELECT_UNPUBLISHED, (batch_size,))
        rows = cur.fetchall()
        if not rows:
            return {'selected': 0, 'published': 0, 'dead_lettered': 0, 'failed': 0}

        rejects = []
        for row in rows:
            event = to_event(row)
            callback = partial(_on_delivery, row['id'], delivered, failed)
            try:
                try:
                    producer.produce(topic=topic, key=event['symbol'], value=event,
                                     on_delivery=callback)
                except BufferError:
                    producer.flush(30)
                    producer.produce(topic=topic, key=event['symbol'], value=event,
                                     on_delivery=callback)
            except (SerializationError, ValueError, TypeError) as exc:
                # Poison record: park it in the DLQ so it cannot block the pipeline.
                dlq.produce(dlq_topic, key=event['symbol'],
                            value=json.dumps({'reason': str(exc), 'event': event}))
                rejects.append((event, f'publish failed: {exc}'))
                dead.add(row['id'])
            producer.poll(0)

        producer.flush(60)
        dlq.flush(30)
        insert_rejects(cur, 'publisher', rejects)

        done_ids = list(delivered | dead)
        if done_ids:
            cur.execute('UPDATE trades SET published_at = now() WHERE id = ANY(%s)',
                        (done_ids,))
        unresolved = len(rows) - len(done_ids)
        if unresolved:
            log.warning('%d rows left unpublished (will retry): %s', unresolved,
                        list(failed.values())[:3])
    return {'selected': len(rows), 'published': len(delivered),
            'dead_lettered': len(dead), 'failed': unresolved}


def run_publisher(max_batches: int = 20) -> dict:
    topic = os.environ.get('KAFKA_TOPIC_TRADES', 'crypto.trades.v1')
    dlq_topic = os.environ.get('KAFKA_TOPIC_DLQ', 'crypto.trades.dlq')
    batch_size = int(os.environ.get('PUBLISH_BATCH_SIZE', '5000'))
    producer, dlq = build_producers()
    totals = {'selected': 0, 'published': 0, 'dead_lettered': 0, 'failed': 0}
    for _ in range(max_batches):
        stats = publish_batch(producer, dlq, topic, dlq_topic, batch_size)
        for key in totals:
            totals[key] += stats[key]
        if stats['selected'] < batch_size or stats['failed']:
            break
    log.info('Publisher totals: %s', totals)
    return totals


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print(run_publisher())
