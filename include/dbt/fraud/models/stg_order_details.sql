with source as (

    select * from {{ source('northwind_lite_data','order_details') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source