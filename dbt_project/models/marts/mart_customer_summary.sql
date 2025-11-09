{{
  config(
    materialized='table',
    schema='analytics'
  )
}}

select
    customer_id,
    count(distinct order_id) as total_orders,
    count(distinct case when is_completed = 1 then order_id end) as completed_orders,
    sum(amount) as total_amount,
    sum(completed_amount) as total_completed_amount,
    avg(amount) as avg_order_value,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    current_timestamp() as calculated_at
from {{ ref('fact_orders') }}
group by customer_id
