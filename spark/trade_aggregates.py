import os
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName("kraken-trade-aggregates")
         .config("spark.cassandra.connection.host", os.environ["CASSANDRA_HOST"])
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("trade_id", T.LongType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("volume", T.DoubleType()),
    T.StructField("side", T.StringType()),
    T.StructField("trade_time", T.DoubleType()),
    T.StructField("source", T.StringType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
       .option("subscribe", os.environ["KAFKA_TOPIC_TRADES"])
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

trades = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("t"))
          .select("t.*")
          .where(F.col("symbol").isNotNull() & F.col("price").isNotNull())
          .withColumn("event_time", F.col("trade_time").cast("timestamp")))

# Drop duplicates caused by at-least-once publishing
deduped = (trades.withWatermark("event_time", "2 minutes")
           .dropDuplicates(["symbol", "trade_id", "event_time"]))

agg = (deduped.groupBy(F.window("event_time", "1 minute"), "symbol")
       .agg(F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.stddev_samp("price").alias("volatility"),
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap"),
            F.sum("volume").alias("total_volume"),
            F.sum(F.when(F.col("side") == "buy", F.col("volume")).otherwise(0.0)).alias("buy_volume"),
            F.count("*").alias("trade_count")))

out = (agg.select(
    "symbol",
    F.to_date(F.col("window.start")).alias("day"),
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    "avg_price", "min_price", "max_price", "volatility", "vwap",
    "total_volume", "buy_volume", "trade_count"))

def write_to_cassandra(batch_df, batch_id):
    (batch_df.write.format("org.apache.spark.sql.cassandra")
     .mode("append")
     .options(keyspace=os.environ["CASSANDRA_KEYSPACE"], table="trade_stats_1m")
     .save())

query = (out.writeStream.outputMode("update")
         .foreachBatch(write_to_cassandra)
         .option("checkpointLocation", "/checkpoints/trade_stats_1m")
         .start())
query.awaitTermination()