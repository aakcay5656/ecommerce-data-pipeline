{{
  config(
    materialized='view',
    schema='staging'
  )
}}

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    cast(amount as float64) as amount,
    product,
    status,
    current_timestamp() as loaded_at
from {{ source('raw_data', 'orders') }}
where amount > 0
