-- This file defines the dbt model named stg_orders; the name comes from this filename.
-- dbt_project.yml makes this staging model a view in the STAGING schema.
-- A view saves this query in Snowflake; it reads current RAW data when another model uses it.
-- Start choosing the RAW columns that the revenue model will use.
select
    -- Keep the original purchase ID so the next model can identify the same purchase on a retry.
    transaction_id,
    -- Keep the already-UTC purchase time as a timestamp without timezone metadata.
    -- This cast sets its data type; it does not itself convert a local time to UTC.
    ordered_at::timestamp_ntz as ordered_at,
    -- Keep the store name so the dashboard can compare shops.
    store,
    -- Keep the purchased product's name so the dashboard can compare products.
    product,
    -- Keep its group, such as Drinks, Food, or Bakery, for grouped reporting.
    category,
    -- Keep how many items the customer bought; dbt uses this to calculate revenue.
    quantity,
    -- Keep the price charged per item when the order was placed.
    unit_price,
    -- Keep the discount percentage so the next model can calculate the amount paid.
    discount_pct,
    -- This column says manual or simulator; it is unrelated to the dbt source() function below.
    source,
    -- Keep the RAW arrival time as a timestamp; the incremental model uses it to select new arrivals.
    loaded_at::timestamp_ntz as loaded_at
-- source() is a built-in dbt function for finding a table that already exists outside dbt's models.
-- Its first argument, retail_raw, is an alias declared in dbt/models/sources.yml, not a database name.
-- Its second argument, ORDERS, selects the table listed under that alias in sources.yml.
-- That YAML supplies database=RETAIL_DEMO and schema=RAW, so this becomes RETAIL_DEMO.RAW.ORDERS.
-- Using this declaration lets dbt track where the data comes from without hard-coding the database here.
from {{ source('retail_raw', 'ORDERS') }}
