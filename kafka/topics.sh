kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic kraken.trades.v1 --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic kraken.trades.dlq --partitions 1 --replication-factor 1