-- Mixing local time and UTC can make a transformation appear to happen before loading.
-- Both columns must use UTC, and a completed transformation must follow its load.
select transaction_id
from {{ ref('fact_orders') }}
where transformed_at < loaded_at
