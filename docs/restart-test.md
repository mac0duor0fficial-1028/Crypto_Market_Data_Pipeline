# Controlled restart test (evidence template)

Goal: prove a restart loses no analytical results and doubles none.

## Procedure

1. Start the stack and let it run for at least **25 minutes** (the check skips the newest
   15 minutes, which may still be inside the watermark).
2. Baseline: `make verify` -> expect `RESULT: PASS`. Paste output below.
3. Kill things mid-flight:
   ```bash
   docker compose stop spark-streaming          # Spark down, Kafka + Airflow keep running
   sleep 180                                     # backlog accumulates in Kafka
   docker compose kill airflow-scheduler         # abrupt stop of the publisher DAG mid-batch
   docker compose up -d airflow-scheduler spark-streaming
   ```
4. Wait 20 minutes so every affected minute is outside the watermark window, then
   `make verify` again.
5. Repeat step 3 with `docker compose restart kafka` and with `docker compose stop cassandra`
   (Spark retries; the checkpoint does not advance past unwritten batches).

## Why it should pass

| Failure | Recovery mechanism |
|---|---|
| Spark stops | Offsets + state in `/checkpoints`; resumes from last committed batch |
| Publisher dies after Kafka ack, before `UPDATE` | Row re-sent; Spark de-dupes on `event_id`; Cassandra upsert |
| Airflow run overlaps / re-runs | `UNIQUE (symbol, trade_id)` + `ON CONFLICT DO NOTHING`; cursor committed with data |
| Cassandra down | `foreachBatch` throws -> batch not committed -> replayed, upserts are idempotent |

## Results (fill in)

```text
<paste make verify output: baseline>
<paste make verify output: after Spark restart>
<paste make verify output: after publisher kill>
```

Known, documented exceptions: events arriving later than `WATERMARK` (default 10 min) behind the
newest event time are dropped by Spark by design; they would show as `Mismatched` minutes.
