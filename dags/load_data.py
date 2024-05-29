from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

@dag(
    start_date=datetime(2024,5,14),
    schedule='@daily',
    catchup=False,
    tags=['airbyte','airflow'],
)

def dataIngestion():
    csv_to_postgres = AirbyteTriggerSyncOperator(
        task_id='ingest_csv_to_postgres',
        airbyte_conn_id='airbyte_conn',
        connection_id='bb315ec0-86c6-4db8-acde-6b655b80be75',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    postgres_to_bigquery = AirbyteTriggerSyncOperator(
        task_id='ingest_postgres_to_bigquery',
        airbyte_conn_id='airbyte_conn',
        connection_id='b1016cab-07de-499c-84f2-abfc1abdf819',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    csv_to_postgres >> postgres_to_bigquery
    
dataIngestion()
