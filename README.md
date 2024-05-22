# Hands-on Airbyte, dbt, Apache Airflow, PostgreSQL and BigQuery
This repository for course Data Product II in Partnatech. 
In this course use tech stack : 
- Data Ingestion : Airbyte
- Data Transformation : dbt
- Workflow Orchestions : Apache Airflow
- Data Platform : OLTP(PostgreSQL) & OLAP(BigQuery)

## Prerequisite
1) Already installed docker and docker compose
2) Already have service account key json file for Google BigQuery
3) Already installed postgresql in local
4) Already installed vscode or other IDE

## Table of Content
1) ELT Data Architecture & Data Modelling
2) Setup Airbyte in docker
3) Define source connection in Airbyte
4) Define destination connection in Airbyte
5) Configure connection in Airbyte
6) Create Airbyte Connection in Airflow Web Server
7) Create airflow dags for trigger Airbyte job
8) Create dbt model
9) Create dags for integration airbyte, dbt and apache airflow

### ELT Data Architecture & Data Modelling
#### ELT Data Architecture
![data_architecture](https://github.com/saipulrx/airbyte-dbt-airflow-demo/blob/main/assets/elt_data_architecture_modelling.png)

#### Data Modelling
In this course, for Data Modelling use Kimbal's Method - Star Schema

<b>ERD Conceptual</b>
![erd conceptual](https://github.com/saipulrx/dbt-snowflake/blob/main/assets/erd-conceptual.drawio.png)

<b>ERD Logical</b>
![erd logical](https://github.com/saipulrx/dbt-snowflake/blob/main/assets/erd-logical.drawio.png)

<b>ERD Physical</b>
<br />
![erd physical](https://github.com/saipulrx/dbt-snowflake/blob/main/assets/erd-physical.drawio.png)

### Setup Airbyte in docker
- git clone this code to local
- run docker compose
  ```
  docker compose up -d
  ``` 
- If success then open url http://localhost:8000 for Airbyte UI

### Define source connection in Airbyte
- Click New Connection
- In Define source, choose setup new source
- Input csv in search text box then click File
- Input dataset name and choose file format csv 
- For Storage Provider choose HTTPS : Public Web and input URL : https://storage.googleapis.com/raw_data_alterra_batch2_2024/customer.csv 
- Click set up source 

### Define destination connection in Airbyte
- In Define destination, choose setup new destination
- Input postgre in search text box then click Postgres
- Input destination name
- Input host : host.docker.internal if postgres db installed in local computer not in docker
- Input port : 5432
- Input DB name
- Input User and password
- Click Setup destination

### Configure connection in Airbyte
- In Connection, input connection name
- In Configuration, Choose schedule type manual(because airbyte job will trigger by airflow)
- Click Setup connection
- Click Sync Now

### Create Airbyte Connection in Airflow Web Server
- Click Admin --> Connections
- Input connection id
- Choose connection type : Airbyte
- Input host : airbyte-server
- Inport port : 8001
- Click Test
- Click Save

### Create airflow dags for trigger Airbyte job
- Go to Airbyte UI then copy connection id for each connection. e.g : b1016cab-07de-499c-84f2-abfc1abdf819
- Copy paste code bellow :
```
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
        task_id='ingest_csv_to_postgres',
        airbyte_conn_id='airbyte_conn',
        connection_id='b1016cab-07de-499c-84f2-abfc1abdf819',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    csv_to_postgres >> postgres_to_bigquery
    
dataIngestion()

```
- Change connection_id in AirbyteTriggerSyncOperator based on previous step
- Enable airflow dag then check data in postgres or BigQuery

### Create dbt model
- Copy paste all files in model folder

### Create dags for integration airbyte, dbt and apache airflow
- Go to Airbyte UI then copy connection id for each connection. e.g : b1016cab-07de-499c-84f2-abfc1abdf819
- Copy paste dags code elt_datapipelines.py in folder dags to your local folder
- Enable airflow dag then check data in postgres or BigQuery
