"""Check a real running order UI for safe retries and rejected bad purchases.

Run after Start-Demo.ps1: python scripts/smoke_test.py
These are actual synthetic purchases, not tests of a mocked database.
"""
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from uuid import uuid4

def call(path, body=None):
    encoded = json.dumps(body).encode() if body is not None else None
    request = Request('http://localhost:8000' + path, data=encoded,
                      headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=15) as response:
        return json.load(response)

# The same request UUID must produce the same transaction ID on a retry.
order = {'request_id': str(uuid4()), 'store': 'Chicago Loop',
         'product': 'Coffee', 'quantity': 2, 'discount_pct': 10}
first = call('/api/orders', order)
replayed = call('/api/orders', order)
assert first['transaction_id'] == replayed['transaction_id'], 'A retry created a duplicate!'
assert first['unit_price'] == '4.50' or float(first['unit_price']) == 4.5

# The server must reject values outside the catalog and quantity limits.
for invalid, expected in [({**order, 'product': 'Unknown'}, 400),
                          ({**order, 'quantity': 0}, 422)]:
    try:
        call('/api/orders', invalid)
        raise AssertionError('Invalid input was accepted.')
    except HTTPError as error:
        assert error.code == expected, f'Expected {expected}, got {error.code}'
print(f'PASS: idempotent order #{first["transaction_id"]}; bad product and quantity rejected.')
print('Next, find this ID in Snowflake/Power BI after the next successful Airflow run.')
