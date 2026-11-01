from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from confluent_kafka.admin import AdminClient
import os

DATABRICKS_CONN = "databricks_default"
SNOWFLAKE_CONN = "snowflake_default"
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "your-cluster-id")

def check_confluent_health():
    admin = AdminClient({
        'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('CONFLUENT_API_KEY'),
        'sasl.password': os.getenv('CONFLUENT_API_SECRET')
    })
    meta = admin.list_topics(timeout=10)
    topic = os.getenv('CONFLUENT_TOPIC', 'trips.raw')
    if topic not in meta.topics:
        raise ValueError(f"Confluent topic '{topic}' not found on cluster!")
    print(f"Confluent Cloud topic '{topic}' active with {len(meta.topics[topic].partitions)} partitions.")

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='nyc_taxi_confluent_pipeline',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    t0_health = PythonOperator(
        task_id='verify_confluent_connectivity',
        python_callable=check_confluent_health,
    )

    t1_bronze = DatabricksNotebookOperator(
        task_id='ingest_bronze',
        databricks_conn_id=DATABRICKS_CONN,
        existing_cluster_id=CLUSTER_ID,
        notebook_path='/Repos/nyc-taxi-pipeline/databricks/01_bronze',
    )

    t2_silver = DatabricksNotebookOperator(
        task_id='process_silver_quarantine',
        databricks_conn_id=DATABRICKS_CONN,
        existing_cluster_id=CLUSTER_ID,
        notebook_path='/Repos/nyc-taxi-pipeline/databricks/02_silver',
    )

    t3_gold = DatabricksNotebookOperator(
        task_id='aggregate_gold',
        databricks_conn_id=DATABRICKS_CONN,
        existing_cluster_id=CLUSTER_ID,
        notebook_path='/Repos/nyc-taxi-pipeline/databricks/03_gold',
    )

    t4_snowflake_merge = SnowflakeOperator(
        task_id='snowflake_merge_facts',
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
        MERGE INTO TAXI_DB.PROD.FACT_TRIPS target
        USING TAXI_DB.STAGING.STAGING_FACT_TRIPS source
        ON target.trip_id = source.trip_id
        WHEN NOT MATCHED THEN INSERT (
            trip_id, vendor_id, pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, fare_amount, total_amount,
            pickup_location_id, dropoff_location_id, payment_type
        ) VALUES (
            source.trip_id, source.vendor_id, source.pickup_datetime, source.dropoff_datetime,
            source.passenger_count, source.trip_distance, source.fare_amount, source.total_amount,
            source.pickup_location_id, source.dropoff_location_id, source.payment_type
        );
        TRUNCATE TABLE TAXI_DB.STAGING.STAGING_FACT_TRIPS;
        """,
    )

    t0_health >> t1_bronze >> t2_silver >> t3_gold >> t4_snowflake_merge
