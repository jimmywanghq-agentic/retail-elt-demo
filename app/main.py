"""A small pretend shop. Both people and the simulator use the same insert path."""
import asyncio
import os
import random
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

# Prices live on the server, so the browser cannot accidentally invent prices.
PRODUCTS = {
    'Coffee': ('Drinks', 4.50), 'Tea': ('Drinks', 3.25),
    'Sandwich': ('Food', 8.75), 'Salad': ('Food', 9.50),
    'Muffin': ('Bakery', 3.75), 'Cookie': ('Bakery', 2.50),
}
STORES = ['Chicago Loop', 'Lakeview', 'Hyde Park']
simulation = {'enabled': False, 'seconds': 5}

class Order(BaseModel):
    # Reusing a request ID prevents a double click or retry from making two orders.
    request_id: UUID = Field(default_factory=uuid4)
    store: str
    product: str
    quantity: int = Field(default=1, ge=1, le=20)
    discount_pct: int = Field(default=0, ge=0, le=50)

class Simulation(BaseModel):
    enabled: bool
    seconds: int = Field(default=5, ge=1, le=60)

def connect():
    return psycopg.connect(os.environ['SOURCE_DSN'], row_factory=dict_row)

def insert_order(order: Order, source='manual'):
    category, price = PRODUCTS[order.product]
    with connect() as conn:
        # Parameters keep submitted values separate from SQL instructions.
        row = conn.execute('''
            INSERT INTO orders(request_id,store,product,category,quantity,unit_price,discount_pct,source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(request_id) DO UPDATE SET request_id=EXCLUDED.request_id
            RETURNING *
        ''', (order.request_id, order.store, order.product, category,
              order.quantity, price, order.discount_pct, source)).fetchone()
    return row

async def generate_orders():
    while True:
        await asyncio.sleep(simulation['seconds'])
        if simulation['enabled']:
            order = Order(store=random.choice(STORES), product=random.choice(list(PRODUCTS)),
                          quantity=random.choices([1,2,3,4], weights=[6,3,2,1])[0],
                          discount_pct=random.choices([0,10,20], weights=[8,2,1])[0])
            # Database work goes to a thread so the UI stays responsive.
            try:
                await asyncio.to_thread(insert_order, order, 'simulator')
            except psycopg.Error as error:
                print(f'Simulator will retry on its next tick: {type(error).__name__}', flush=True)

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(generate_orders())
    yield
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

app = FastAPI(title='Retail order simulator', lifespan=lifespan)

@app.get('/', response_class=HTMLResponse)
def home():
    return Path('/opt/demo/app/index.html').read_text(encoding='utf-8')

@app.get('/api/catalog')
def catalog():
    return {'products': PRODUCTS, 'stores': STORES}

@app.post('/api/orders')
def place_order(order: Order):
    from fastapi import HTTPException
    if order.product not in PRODUCTS or order.store not in STORES:
        raise HTTPException(400, 'Choose a product and store from the catalog.')
    return insert_order(order)

@app.post('/api/simulator')
def change_simulator(settings: Simulation):
    simulation.update(settings.model_dump())
    return simulation

@app.get('/api/status')
def status():
    with connect() as conn:
        counts = conn.execute('''SELECT count(*) AS orders,
            count(*) FILTER (WHERE delivered_at IS NULL) AS pending,
            max(delivered_at) AS last_delivered_at FROM orders''').fetchone()
        rows = conn.execute('SELECT * FROM orders ORDER BY transaction_id DESC LIMIT 15').fetchall()
    # Delivered means RAW received the order. Airflow/dbt completion is a later step.
    return {'counts': counts, 'recent': rows, 'simulator': simulation}
