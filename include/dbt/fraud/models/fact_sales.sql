{{ config(
    partition_by={
      "field": "order_date",
      "data_type": "date"
    }
)}}
with source as(
    select
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        date(o.order_date) as order_date,
        o.shipped_date,
        o.paid_date,
        current_timestamp() as insertion_timestamp,
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_order_details') }} od
    on od.order_id = o.id
    where od.order_id is not null
)
select * 
from source