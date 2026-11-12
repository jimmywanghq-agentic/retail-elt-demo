-- A dbt test passes when this query returns zero incorrect orders.
select transaction_id
from {{ ref('fact_orders') }}
where quantity <= 0 or unit_price <= 0 or net_revenue < 0
   or abs(net_revenue - round(quantity * unit_price * (1 - discount_pct / 100.0), 2)) > 0.01
