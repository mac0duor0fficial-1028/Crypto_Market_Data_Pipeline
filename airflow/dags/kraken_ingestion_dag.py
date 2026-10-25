"""Every minute: register symbols, pull new Kraken trades per pair, snapshot tickers.

Tasks run sequentially on purpose: they share one outbound IP and Kraken's public
REST rate limit, and the client enforces a minimum spacing between calls.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.config import kraken_pairs

PAIRS = kraken_pairs()


def _ensure_symbols(**_):
    from src.ingestion import ensure_symbols
    return ensure_symbols(PAIRS)


def _ingest_trades(pair, **_):
    from src.ingestion import ingest_trades
    return ingest_trades(pair)


def _ingest_tickers(**_):
    from src.ingestion import ingest_ticker
    return [ingest_ticker(p) for p in PAIRS]


with DAG(
    dag_id='kraken_trades_ingestion',
    description='Kraken REST -> PostgreSQL staging (incremental, idempotent)',
    start_date=datetime(2026, 1, 1),
    schedule='* * * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args={'retries': 2, 'retry_delay': timedelta(seconds=15),
                  'execution_timeout': timedelta(minutes=2)},
    tags=['crypto', 'ingestion'],
) as dag:
    seed = PythonOperator(task_id='ensure_symbols', python_callable=_ensure_symbols)
    trade_tasks = [
        PythonOperator(task_id=f'ingest_trades_{pair}', python_callable=_ingest_trades,
                       op_kwargs={'pair': pair}, trigger_rule=TriggerRule.ALL_DONE)
        for pair in PAIRS
    ]
    ticker = PythonOperator(task_id='ingest_tickers', python_callable=_ingest_tickers,
                            trigger_rule=TriggerRule.ALL_DONE)
    chain(seed, *trade_tasks, ticker)
