import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.kraken_client import KrakenClient
from src.validation import validate_trade
from src import db
from src.publisher import publish_unpublished

PAIRS = os.environ["KRAKEN_PAIRS"].split(",")

@dag(
    dag_id="kraken_ingest",
    schedule="* * * * *",          # every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,               # never overlap runs
    default_args={"retries": 3, "retry_delay": timedelta(seconds=20)},
)
def kraken_ingest():

    @task
    def ingest(pair: str):
        client = KrakenClient()
        with db.connect() as conn:
            cursor = db.get_cursor(conn, pair)           # last 'since' value
            rows, last = client.trades(pair, since=cursor)
            good, bad = [], []
            for r in rows:
                try:
                    good.append(validate_trade(r))
                except ValueError as e:
                    bad.append((pair, str(e)))
            db.insert_trades(conn, pair, good)           # ON CONFLICT DO NOTHING
            db.insert_rejects(conn, bad)
            db.set_cursor(conn, pair, last)              # same transaction
        return {"pair": pair, "inserted": len(good), "rejected": len(bad)}

    @task
    def publish():
        return publish_unpublished()

    results = ingest.expand(pair=PAIRS)   # one mapped task per pair
    results >> publish()

kraken_ingest()