"""Kafka -> Spark Structured Streaming -> Cassandra.

Queries (each with its own checkpoint):
  trade_agg_1m  1-minute event-time windows (+ anomaly detection)
  trade_agg_1h  1-hour windows
  latest_price  latest trade per symbol

Correctness model: Kafka delivery is at-least-once, so events are de-duplicated on
event_id within the watermark; sinks are Cassandra upserts keyed by window, so replaying
a micro-batch after a restart rewrites identical rows (no lost or doubled results).
"""
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('stream_trades')

CASS = 'org.apache.spark.sql.cassandra'
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = os.environ.get('KAFKA_TOPIC_TRADES', 'crypto.trades.v1')
KEYSPACE = os.environ.get('CASSANDRA_KEYSPACE', 'crypto')
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cassandra')
WATERMARK = os.environ.get('WATERMARK', '10 minutes')
TRIGGER = os.environ.get('TRIGGER_INTERVAL', '30 seconds')
CHECKPOINT_DIR = os.environ.get('CHECKPOINT_DIR', '/checkpoints')
SCHEMA_PATH = os.environ.get('AVRO_SCHEMA_PATH', '/opt/schemas/trade_event.avsc')
RANGE_PCT = float(os.environ.get('ANOMALY_RANGE_PCT', '1.0'))
VOLUME_Z = float(os.environ.get('ANOMALY_VOLUME_Z', '4.0'))
MIN_TRADES = int(os.environ.get('ANOMALY_MIN_TRADES', '5'))
MAX_OFFSETS = os.environ.get('MAX_OFFSETS_PER_TRIGGER')

AVRO_SCHEMA = open(SCHEMA_PATH).read()


def build_spark():
    return (SparkSession.builder.appName('crypto-trades-streaming')
            .config('spark.cassandra.connection.host', CASSANDRA_HOST)
            .config('spark.sql.session.timeZone', 'UTC')  # day/month buckets are UTC
            .config('spark.sql.shuffle.partitions', os.environ.get('SPARK_SHUFFLE_PARTITIONS', '8'))
            .getOrCreate())


def read_events(spark):
    """Decode Confluent-framed Avro: 1 magic byte + 4-byte schema id, then the payload."""
    reader = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')   # only used on the very first run
              .option('failOnDataLoss', 'false'))
    if MAX_OFFSETS:
        reader = reader.option('maxOffsetsPerTrigger', MAX_OFFSETS)
    raw = reader.load()
    decoded = raw.select(F.from_avro(F.expr('substring(value, 6, length(value) - 5)'),
                                     AVRO_SCHEMA, {'mode': 'PERMISSIVE'}).alias('t'))
    return (decoded.filter(F.col('t.event_id').isNotNull())      # drops undecodable records
            .select('t.*')
            .withColumn('event_time', (F.col('trade_ts_ms') / 1000).cast('timestamp')))


def deduplicated_trades(spark):
    return (read_events(spark)
            .withWatermark('event_time', WATERMARK)
            .dropDuplicatesWithinWatermark(['event_id']))


def aggregate(trades, duration):
    return (trades.groupBy(F.window('event_time', duration), 'symbol')
            .agg(F.avg('price').alias('avg_price'),
                 F.min('price').alias('min_price'),
                 F.max('price').alias('max_price'),
                 (F.sum(F.col('price') * F.col('volume')) / F.sum('volume')).alias('vwap'),
                 F.sum('volume').alias('volume'),
                 F.count(F.lit(1)).alias('trade_count'),
                 # volatility = sample std-dev of trade prices in the window (0 if 1 trade)
                 F.coalesce(F.stddev('price'), F.lit(0.0)).alias('volatility')))


def cassandra_write(df, table):
    (df.write.format(CASS).options(keyspace=KEYSPACE, table=table).mode('append').save())


METRICS = ['avg_price', 'min_price', 'max_price', 'vwap', 'volume', 'trade_count', 'volatility']


def detect_anomalies(batch_df):
    """Rule 1: intra-minute price range above ANOMALY_RANGE_PCT.
    Rule 2: minute volume z-score above ANOMALY_VOLUME_Z vs the preceding 30 minutes."""
    spark = batch_df.sparkSession
    rows = batch_df.filter(F.col('trade_count') >= MIN_TRADES)

    range_pct = (F.col('max_price') - F.col('min_price')) / F.col('avg_price') * 100
    price_anoms = (rows.withColumn('range_pct', range_pct)
                   .filter(F.col('range_pct') > RANGE_PCT)
                   .select('symbol', 'day', 'window_start',
                           F.lit('PRICE_RANGE').alias('kind'),
                           F.format_string('range=%.3f%% min=%.2f max=%.2f',
                                           'range_pct', 'min_price', 'max_price').alias('details')))

    hist = (spark.read.format(CASS).options(keyspace=KEYSPACE, table='trade_agg_1m').load()
            .select(F.col('symbol').alias('h_symbol'),
                    F.col('window_start').alias('h_ws'),
                    F.col('volume').alias('h_vol')))
    cond = ((F.col('symbol') == F.col('h_symbol'))
            & (F.col('h_ws') < F.col('window_start'))
            & (F.col('h_ws') >= F.col('window_start') - F.expr('INTERVAL 30 MINUTES')))
    stats = (rows.join(hist, cond, 'left')
             .groupBy('symbol', 'day', 'window_start', 'volume')
             .agg(F.avg('h_vol').alias('mu'), F.stddev('h_vol').alias('sd'),
                  F.count('h_vol').alias('n')))
    vol_anoms = (stats.filter((F.col('n') >= 10) & (F.col('sd') > 0))
                 .withColumn('z', (F.col('volume') - F.col('mu')) / F.col('sd'))
                 .filter(F.col('z') > VOLUME_Z)
                 .select('symbol', 'day', 'window_start',
                         F.lit('VOLUME_SPIKE').alias('kind'),
                         F.format_string('volume=%.4f z=%.2f mean=%.4f',
                                         'volume', 'z', 'mu').alias('details')))
    return price_anoms.unionByName(vol_anoms)


def write_1m(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    out = batch_df.select(
        'symbol',
        F.to_date(F.col('window.start')).alias('day'),
        F.col('window.start').alias('window_start'),
        *METRICS).persist()
    try:
        cassandra_write(out, 'trade_agg_1m')
        log.info('batch %s: wrote %d 1m windows', batch_id, out.count())
        try:  # anomalies must never block the core aggregates
            cassandra_write(detect_anomalies(out), 'anomalies')
        except Exception:
            log.exception('batch %s: anomaly detection failed (aggregates were written)', batch_id)
    finally:
        out.unpersist()


def write_1h(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    cassandra_write(batch_df.select(
        'symbol',
        F.date_format(F.col('window.start'), 'yyyy-MM').alias('month'),
        F.col('window.start').alias('window_start'),
        *METRICS), 'trade_agg_1h')


def write_latest(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    cassandra_write(batch_df.select('symbol', 'price', 'trade_ts'), 'latest_price')


def start(name, df, sink):
    return (df.writeStream.queryName(name).outputMode('update')
            .foreachBatch(sink)
            .option('checkpointLocation', f'{CHECKPOINT_DIR}/{name}')
            .trigger(processingTime=TRIGGER).start())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel('WARN')
    log.info('Starting streams: topic=%s watermark=%s trigger=%s', TOPIC, WATERMARK, TRIGGER)

    start('trade_agg_1m', aggregate(deduplicated_trades(spark), '1 minute'), write_1m)
    start('trade_agg_1h', aggregate(deduplicated_trades(spark), '1 hour'), write_1h)
    latest = (read_events(spark).groupBy('symbol')
              .agg(F.max_by('price', 'event_time').alias('price'),
                   F.max('event_time').alias('trade_ts')))
    start('latest_price', latest, write_latest)

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
