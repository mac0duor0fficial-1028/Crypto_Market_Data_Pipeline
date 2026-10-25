-- Runs once on first start of the Postgres volume (docker-entrypoint-initdb.d).
-- Airflow keeps its metadata in a separate database from the staging data.
CREATE DATABASE airflow_meta;
