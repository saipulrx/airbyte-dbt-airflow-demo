FROM apache/airflow:2.8.2

RUN pip install --no-cache-dir apache-airflow-providers-airbyte==3.6.0
RUN pip install --no-cache-dir astronomer-cosmos[dbt-snowflake,dbt-bigquery]==1.3.2

ENV PIP_USER=false

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.7.2 && deactivate

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery==1.7.6 && deactivate

ENV PIP_USER=true
