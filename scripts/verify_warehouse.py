"""Compare the real PostgreSQL source with RAW and dbt facts after the queue drains.

Stop automatic orders, wait for a successful scheduled run, then execute:
docker compose exec orders-ui python scripts/verify_warehouse.py
"""
import json
import os
import psycopg
from pipeline.snowflake_connection import connect

with psycopg.connect(os.environ['SOURCE_DSN']) as source:
    # Use the same money calculation as the business rule, independently in PostgreSQL.
    source_count, pending, source_revenue = source.execute('''
        SELECT count(*), count(*) FILTER (WHERE delivered_at IS NULL),
               coalesce(sum(round(quantity*unit_price*(1-discount_pct/100.0),2)),0)
        FROM orders
    ''').fetchone()

with connect() as warehouse:
    cursor = warehouse.cursor()
    raw_count, raw_distinct = cursor.execute(
        'SELECT count(*), count(distinct transaction_id) FROM RAW.ORDERS').fetchone()
    fact_count, fact_distinct, fact_revenue, invalid_times = cursor.execute('''
        SELECT count(*), count(distinct transaction_id), coalesce(sum(net_revenue),0),
               count_if(transformed_at < loaded_at)
        FROM MARTS.FACT_ORDERS
    ''').fetchone()

result = {'source_orders': source_count, 'pending_orders': pending,
          'raw_orders': raw_count, 'raw_unique_ids': raw_distinct,
          'fact_orders': fact_count, 'fact_unique_ids': fact_distinct,
          'source_revenue': str(source_revenue), 'fact_revenue': str(fact_revenue),
          'invalid_utc_timestamps': invalid_times or 0}
print(json.dumps(result, indent=2))
# These checks find missing rows, duplicate rows, wrong money, and the timezone bug.
assert pending == 0, 'Wait for the PostgreSQL delivery queue to drain.'
assert source_count == raw_count == raw_distinct == fact_count == fact_distinct
assert source_revenue == fact_revenue
assert not invalid_times
print('PASS: every source order reached the dbt facts exactly once, with matching revenue.')
