{# Keep the existing table and process new arrivals on later runs, instead of rebuilding all history. #}
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}
{# Above: transaction_id identifies an existing purchase; merge updates matching IDs or inserts missing IDs. #}

-- Start choosing the columns that Power BI will see in the reporting table.
select
    -- Keep the original purchase ID so repeated batches cannot create another copy of the same purchase.
    transaction_id,
    -- Keep the original purchase time so the report shows when the customer bought the product.
    ordered_at,
    -- Round the purchase time down to the minute so Power BI can group sales by minute.
    date_trunc('minute', ordered_at) as order_minute,
    -- Keep the shop name so the report can compare stores.
    store,
    -- Keep the product name so the report can show which products sell.
    product,
    -- Keep the product group so food, drinks, and bakery sales can be compared.
    category,
    -- Keep the number of items bought so we can calculate the purchase amount.
    quantity,
    -- Keep the price charged per item at purchase time, rather than looking up a later price.
    unit_price,
    -- Keep the discount percentage so the discounted amount can be explained.
    discount_pct,
    -- Keep whether the purchase came from the manual order form or the simulator.
    source,
    -- Multiply quantity by price to calculate the amount before discounts; keep two decimal places.
    (quantity * unit_price)::number(14,2) as gross_revenue,
    -- Calculate how many dollars the customer saved; dividing by 100 turns a percentage into a fraction.
    (quantity * unit_price * discount_pct / 100)::number(14,2) as discount_amount,
    -- Calculate the amount paid after the discount; this is the revenue shown in Power BI.
    (quantity * unit_price * (1 - discount_pct / 100.0))::number(14,2) as net_revenue,
    -- Keep the RAW loading time because it tells later runs which arrivals to process.
    loaded_at,
    -- Record this transformation's time in UTC so it can be compared with the loading time.
    convert_timezone('UTC', current_timestamp())::timestamp_ntz as transformed_at
-- Read the staging model; ref also tells dbt that staging must be ready before this model runs.
from {{ ref('stg_orders') }}
{# Only apply the filter below when the reporting table already exists and a full rebuild was not requested. #}
{% if is_incremental() %}
-- Select arrivals at or after the latest loading time already recorded in this reporting table.
-- Use loading time, not purchase time, so a purchase that arrives late is still included.
-- Recheck equal timestamps deliberately; the transaction-ID merge prevents duplicate rows.
-- this means the reporting table being built; coalesce supplies a starting date if that table is empty.
where loaded_at >= (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{# Finish the conditional filter; the first run reads all staging rows to create the initial table. #}
{% endif %}
