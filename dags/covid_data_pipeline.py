from __future__ import annotations
import io
import tempfile
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


S3_BUCKET_NAME = "covid-data-analytics-pranithapadala"
S3_KEY_RAW = "raw/covid_us_states_raw.csv"
S3_KEY_PROCESSED = "processed/covid_us_states_processed.csv"
DATA_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"
DB_TABLE_NAME = "covid_state_metrics"
POSTGRES_CONN_ID = "postgres_default"


@dag(
    dag_id="public_health_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md="ETL pipeline for Public Health Analytics.",
    tags=['Public Health'],
)
def etl_pipeline():

    @task
    def create_postgres_table_if_not_exists():
        """Creates the destination table in PostgreSQL if it's not there."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME} (
                date DATE,
                state VARCHAR(50),
                fips INTEGER,
                cases INTEGER,
                deaths INTEGER,
                new_cases INTEGER,
                new_deaths INTEGER,
                PRIMARY KEY (date, state)
            );
        """
        pg_hook.run(create_table_sql)

    @task
    def extract_data():
        """Downloads the data and returns it as a string."""
        df = pd.read_csv(DATA_URL)
        return df.to_csv(index=False)

    @task
    def stage_raw_data_to_s3(csv_data: str):
        """Uploads the raw data to the S3 Data Lake."""
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data=csv_data,
            key=S3_KEY_RAW,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )

    @task
    def transform_data(csv_data: str):
        """Cleans the data and calculates new metrics."""
        df = pd.read_csv(io.StringIO(csv_data))
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['state', 'date'])
        df['new_cases'] = df.groupby('state')['cases'].diff().fillna(0)
        df['new_deaths'] = df.groupby('state')['deaths'].diff().fillna(0)
        df['new_cases'] = df['new_cases'].clip(lower=0)
        df['new_deaths'] = df['new_deaths'].clip(lower=0)
        int_columns = ['fips', 'cases', 'deaths', 'new_cases', 'new_deaths']
        df[int_columns] = df[int_columns].astype(int)
        return df.to_csv(index=False)

    @task
    def stage_clean_data_to_s3(csv_data: str):
        """Uploads the cleaned data to the S3 Data Lake."""
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data=csv_data,
            key=S3_KEY_PROCESSED,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )

    @task
    def load_clean_data_to_postgres(csv_data: str):
        """Loads the clean data into the PostgreSQL Data Warehouse using a temp CSV file."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(f"TRUNCATE TABLE {DB_TABLE_NAME};", autocommit=True)

        with tempfile.NamedTemporaryFile("w+", delete=True, suffix=".csv") as tmpfile:
            tmpfile.write(csv_data)
            tmpfile.flush()
            pg_hook.copy_expert(
                sql=f"COPY {DB_TABLE_NAME} FROM STDIN WITH CSV HEADER",
                filename=tmpfile.name,
            )

    # Define task order
    table_creation_task = create_postgres_table_if_not_exists()
    raw_data = extract_data()
    staged_raw = stage_raw_data_to_s3(raw_data)
    transformed_data = transform_data(raw_data)
    staged_clean = stage_clean_data_to_s3(transformed_data)
    loaded = load_clean_data_to_postgres(transformed_data)

    raw_data >> [staged_raw, transformed_data]
    transformed_data >> [staged_clean, loaded]
    table_creation_task >> loaded


etl_pipeline()
