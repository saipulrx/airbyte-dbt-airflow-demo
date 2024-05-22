with source as (
    select 
    case 
        when vendorid=1 THEN 'Creative Mobile Technologies'
        else 'VeriFone Inc.'
    end as vendorname,
    trip_distance,
    passenger_count,
    sum(total_amount) as sum_total_amount
    from {{ source('green_trip_data','green_trip_data_2019_jan') }}
    where payment_type = 1
    and trip_distance > 5
    group by vendorname,trip_distance,passenger_count
    order by sum_total_amount desc
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source