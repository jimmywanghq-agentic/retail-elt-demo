-- Keep the original time in UTC so PostgreSQL, Spark and Power BI agree.
select transaction_id, ordered_at::timestamp_ntz as ordered_at,
       store, product, category, quantity, unit_price, discount_pct, source,
       loaded_at::timestamp_ntz as loaded_at
from {{ source('retail_raw', 'ORDERS') }}
