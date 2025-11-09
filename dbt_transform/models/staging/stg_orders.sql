select
    order_id,
    customer_id,
    order_date,
    cast(amount as float64) as amount
from {{ source('raw_data', 'orders') }}
