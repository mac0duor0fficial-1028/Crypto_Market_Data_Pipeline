"""Every minute: publish unpublished staged trades to Kafka, then run quality checks."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _publish(**_):
    from src.publisher import run_publisher
    return run_publisher(max_batches=20)


def _freshness(**_):
    from src.quality import check_freshness
    return check_freshness()


def _backlog(**_):
    from src.quality import check_backlog
    return check_backlog()


def _reject_rate(**_):
    from src.quality import check_reject_rate
    return check_reject_rate()


with DAG(
    dag_id='pg_to_kafka_publisher',
    description='PostgreSQL staging -> Kafka (Avro, Schema Registry) + data-quality checks',
    start_date=datetime(2026, 1, 1),
    schedule='* * * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args={'retries': 2, 'retry_delay': timedelta(seconds=15),
                  'execution_timeout': timedelta(minutes=3)},
    tags=['crypto', 'kafka'],
) as dag:
    publish = PythonOperator(task_id='publish_to_kafka', python_callable=_publish)
    # Quality checks do not retry: a breach should alert immediately.
    qc_kwargs = {'retries': 0}
    freshness = PythonOperator(task_id='check_freshness', python_callable=_freshness, **qc_kwargs)
    backlog = PythonOperator(task_id='check_backlog', python_callable=_backlog, **qc_kwargs)
    rejects = PythonOperator(task_id='check_reject_rate', python_callable=_reject_rate, **qc_kwargs)
    publish >> [freshness, backlog, rejects]
