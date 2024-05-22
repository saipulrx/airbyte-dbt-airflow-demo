with source as (

    select * from {{ source('northwind_lite_data','orders') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source