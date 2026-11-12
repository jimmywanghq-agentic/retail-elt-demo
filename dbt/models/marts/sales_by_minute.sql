{{ config(materialized='view') }}
-- A view always summarizes the current fact table, including late-arriving orders.
select order_minute, store, count(*) as order_count,
       sum(quantity) as items_sold, sum(net_revenue) as net_revenue
from {{ ref('fact_orders') }}
group by order_minute, store
