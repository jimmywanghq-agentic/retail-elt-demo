-- Airflow uses a separate database so its internal tables do not mix with orders.
CREATE DATABASE airflow;

-- Each row is one purchased product, with an immutable price at purchase time.
CREATE TABLE orders (
    transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE,
    ordered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    store TEXT NOT NULL,
    product TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity BETWEEN 1 AND 20),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    discount_pct INTEGER NOT NULL DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 50),
    source TEXT NOT NULL CHECK (source IN ('manual','simulator')),
    delivered_at TIMESTAMPTZ
);
-- An undelivered row is a durable promise to send this order to Snowflake.
-- This avoids losing late-committing orders when IDs arrive out of order.
CREATE INDEX pending_orders ON orders(transaction_id) WHERE delivered_at IS NULL;
