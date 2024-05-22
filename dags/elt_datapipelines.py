from airflow.utils.dates import days_ago
from airflow.decorators import dag,task, task_group
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models.baseoperator import chain

SUPPLIERS_CONN_ID = 'ae9129e5-2551-42f4-a34d-b1debc0a230a'
PRODUCTS_CONN_ID = 'ee8d493a-8413-40d0-a738-ecaeb5b05bdd'
INV_TRX_CONN_ID = 'a0005643-2f65-4061-abc6-c644cfdc9a6e'
ORDERS_CONN_ID = '8f1302f2-5fed-4115-8734-d3d995a42899'
ORDER_DETAILS_CONN_ID = 'df3d3f45-b43e-4976-a714-1d922de7820a'

@dag(
    start_date=days_ago(1),
    schedule='@daily',
    catchup=False,
    tags=['airbyte','dbt','elt_northwind_data'],
)

def ELT_northwind_data():
    @task_group(group_id='airbyteTaskGroup')
    def EL_proses():
        ingest_suppliers = AirbyteTriggerSyncOperator(
            task_id='ingest_data_suppliers',
            airbyte_conn_id='airbyte_conn',
            connection_id=SUPPLIERS_CONN_ID,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        )

        ingest_products = AirbyteTriggerSyncOperator(
            task_id='ingest_data_products',
            airbyte_conn_id='airbyte_conn',
            connection_id=PRODUCTS_CONN_ID,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        )

        ingest_inv_trx = AirbyteTriggerSyncOperator(
            task_id='ingest_data_inv_trx',
            airbyte_conn_id='airbyte_conn',
            connection_id=INV_TRX_CONN_ID,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        )

        ingest_orders = AirbyteTriggerSyncOperator(
            task_id='ingest_data_orders',
            airbyte_conn_id='airbyte_conn',
            connection_id=ORDERS_CONN_ID,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        )

        ingest_order_details = AirbyteTriggerSyncOperator(
            task_id='ingest_data_order_details',
            airbyte_conn_id='airbyte_conn',
            connection_id=ORDER_DETAILS_CONN_ID,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        )        
        ingest_products >> ingest_suppliers >> ingest_inv_trx >> ingest_orders >> ingest_order_details

    @task
    def airbyte_job_done():
        return True
    
    northwind_data = DbtTaskGroup(
        group_id = "dbtTaskGroup",
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select=['path:models']
        )
    )

    chain(
        EL_proses(),
        airbyte_job_done(),
        northwind_data
    )

ELT_northwind_data()