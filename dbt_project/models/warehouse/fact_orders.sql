{{
  config(
    materialized='table',
    schema='warehouse'
  )
}}

select
    order_id,
    customer_id,
    order_date,
    amount,
    product,
    status,
    case
        when status = 'completed' then amount
        else 0
    end as completed_amount,
    case
        when status = 'completed' then 1
        else 0
    end as is_completed,
    loaded_at
from {{ ref('stg_orders') }}
