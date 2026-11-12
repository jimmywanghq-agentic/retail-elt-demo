"""One connection function prevents slightly different credentials in each script."""
import os
from pathlib import Path
import snowflake.connector

def connect():
    key = Path(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'])
    if not key.is_file():
        raise RuntimeError('Snowflake private key is missing. Complete setup before starting Airflow.')
    # Our demo service key is mounted as a file; it is never written to logs.
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        private_key_file=str(key),
        role=os.environ.get('SNOWFLAKE_ROLE', 'ELT_ROLE'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'ELT_WH'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'RETAIL_DEMO'),
        schema='RAW', login_timeout=30, network_timeout=60,
        session_parameters={'QUERY_TAG': 'retail-elt-demo', 'TIMEZONE': 'UTC'},
    )
