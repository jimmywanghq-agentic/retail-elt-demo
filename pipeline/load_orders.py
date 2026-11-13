"""Send pending PostgreSQL purchases through Spark into Snowflake, safely on retries."""
import os
from datetime import datetime, timezone

import psycopg
from pyspark.sql import SparkSession, functions as F
from snowflake.connector.pandas_tools import write_pandas
from pipeline.snowflake_connection import connect

def load_orders():
    dsn = os.environ['SOURCE_DSN']
    with psycopg.connect(dsn) as source:
        # We use a delivery queue instead of MAX(id). A lower ID can commit late.
        ids = [r[0] for r in source.execute('''SELECT transaction_id FROM orders
            WHERE delivered_at IS NULL ORDER BY transaction_id LIMIT 5000''')]
    if not ids:
        print('No new orders. Nothing to load.')
        # Airflow saves this small result under XCom -> return_value, including on an empty run.
        return {'loaded_orders': 0, 'transaction_ids': [], 'ids_truncated': False}

    spark = (SparkSession.builder.master('local[2]').appName('Retail minute batch')
             .config('spark.jars', '/opt/jdbc/postgresql.jar')
             .config('spark.ui.enabled', 'false')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.driver.memory', '512m').getOrCreate())
    try:
        # IDs came from PostgreSQL as integers, not from user input.
        selected_ids = ','.join(str(int(i)) for i in ids)
        query = f'''(SELECT transaction_id, ordered_at, store, product, category,
            quantity, unit_price, discount_pct, source FROM orders
            WHERE transaction_id IN ({selected_ids})) AS batch'''
        frame = (spark.read.format('jdbc')
                 .option('url', 'jdbc:postgresql://postgres:5432/retail')
                 .option('dbtable', query).option('user', 'demo')
                 .option('password', 'demo_local_only')
                 .option('driver', 'org.postgresql.Driver').load())
        # Keep business transformations in dbt; Spark standardizes the loading shape.
        frame = (frame.dropDuplicates(['transaction_id'])
                 .withColumn('ordered_at', F.date_format('ordered_at', 'yyyy-MM-dd HH:mm:ss.SSSSSS')))
        frame = frame.select([F.col(c).alias(c.upper()) for c in frame.columns])
        # This bounded demo batch fits in memory. Large projects should use staged files.
        data = frame.toPandas()
        data['LOADED_AT'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        with connect() as warehouse:
            cursor = warehouse.cursor()
            # A fixed batch table is safe because Airflow permits only one active run.
            cursor.execute('CREATE TABLE IF NOT EXISTS RAW.ORDERS_BATCH LIKE RAW.ORDERS')
            cursor.execute('TRUNCATE TABLE RAW.ORDERS_BATCH')
            success, _, count, _ = write_pandas(warehouse, data, 'ORDERS_BATCH', schema='RAW')
            if not success or count != len(ids):
                raise RuntimeError('The complete batch was not loaded; PostgreSQL remains unacknowledged.')
            # Replaying a batch inserts only missing transaction IDs.
            cursor.execute('''MERGE INTO RAW.ORDERS t USING RAW.ORDERS_BATCH s
                ON t.TRANSACTION_ID=s.TRANSACTION_ID
                WHEN NOT MATCHED THEN INSERT
                (TRANSACTION_ID,ORDERED_AT,STORE,PRODUCT,CATEGORY,QUANTITY,UNIT_PRICE,DISCOUNT_PCT,SOURCE,LOADED_AT)
                VALUES(s.TRANSACTION_ID,s.ORDERED_AT,s.STORE,s.PRODUCT,s.CATEGORY,
                    s.QUANTITY,s.UNIT_PRICE,s.DISCOUNT_PCT,s.SOURCE,s.LOADED_AT)''')
        with psycopg.connect(dsn) as source:
            # Acknowledge only after Snowflake's MERGE commits successfully.
            # A crash before this point simply replays the same IDs next time.
            source.execute('UPDATE orders SET delivered_at=clock_timestamp() WHERE transaction_id=ANY(%s)', (ids,))
        print(f'Delivered {len(ids)} orders to Snowflake RAW.')
        # Show purchase IDs in the task log so you can search for the order made in the UI.
        print(f'Loaded transaction IDs (first 50): {ids[:50]}')
        # Return only a small summary to Airflow; keep the actual purchase rows in the databases.
        # A large batch lists its first 50 IDs and sets ids_truncated to tell you the list is incomplete.
        return {'loaded_orders': len(ids), 'transaction_ids': ids[:50], 'ids_truncated': len(ids) > 50}
    finally:
        spark.stop()

if __name__ == '__main__':
    load_orders()
