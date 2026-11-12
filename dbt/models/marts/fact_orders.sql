{{ config(materialized='incremental', unique_key='transaction_id', incremental_strategy='merge') }}

-- dbt calculates money after loading: this is the T in ELT.
select transaction_id, ordered_at,
       date_trunc('minute', ordered_at) as order_minute,
       store, product, category, quantity, unit_price, discount_pct, source,
       (quantity * unit_price)::number(14,2) as gross_revenue,
       (quantity * unit_price * discount_pct / 100)::number(14,2) as discount_amount,
       (quantity * unit_price * (1 - discount_pct / 100.0))::number(14,2) as net_revenue,
       -- Set UTC explicitly: the dbt connection can inherit the account timezone.
       loaded_at, convert_timezone('UTC', current_timestamp())::timestamp_ntz as transformed_at
from {{ ref('stg_orders') }}
{% if is_incremental() %}
-- Use load time rather than order time, so late-arriving purchases are included.
-- The inclusive comparison rechecks ties. MERGE keeps them from becoming duplicates.
where loaded_at >= (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
