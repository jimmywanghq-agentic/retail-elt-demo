"""Run inside our container to test the exact credentials used by Spark and dbt."""
from pipeline.snowflake_connection import connect

with connect() as conn:
    result = conn.cursor().execute('''SELECT 1 AS connection_ok,
        CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()''').fetchone()
    print(dict(zip(['connection_ok','account','user','role','warehouse'], result)))
