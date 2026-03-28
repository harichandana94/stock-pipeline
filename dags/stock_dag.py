import os
import requests
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Config ---
TICKERS = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'AMZN']
API_KEY = os.environ.get('POLYGON_API_KEY')
DB_CONN = "host=postgres dbname=stocks user=airflow password=airflow"

# --- Task 1: Create table if it doesn't exist ---
def create_table():
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;

        CREATE TABLE IF NOT EXISTS raw.stock_prices (
            id          SERIAL PRIMARY KEY,
            ticker      VARCHAR(10),
            open        NUMERIC,
            high        NUMERIC,
            low         NUMERIC,
            close       NUMERIC,
            volume      BIGINT,
            trade_date  DATE,
            loaded_at   TIMESTAMP DEFAULT NOW(),
            UNIQUE(ticker, trade_date)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table ready.")

# --- Task 2: Fetch from Polygon.io and load ---
def fetch_and_load():
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    # Yesterday's date (free tier gives previous day data)
    date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    for ticker in TICKERS:
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={API_KEY}"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Skipping {ticker}: {response.status_code} - {response.text}")
            continue

        data = response.json()
        print(f"Fetched {ticker}: close={data.get('close')}")

        cur.execute("""
            INSERT INTO raw.stock_prices
                (ticker, open, high, low, close, volume, trade_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, trade_date) DO NOTHING;
        """, (
            ticker,
            data.get('open'),
            data.get('high'),
            data.get('low'),
            data.get('close'),
            data.get('volume'),
            date
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("All tickers loaded.")

# --- Task 3: Validate the data ---
def validate():
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    # Check 1: any rows loaded?
    cur.execute("SELECT COUNT(*) FROM raw.stock_prices;")
    count = cur.fetchone()[0]
    print(f"Total rows in table: {count}")
    assert count > 0, "No rows found — something went wrong!"

    # Check 2: no nulls in close price
    cur.execute("SELECT COUNT(*) FROM raw.stock_prices WHERE close IS NULL;")
    nulls = cur.fetchone()[0]
    print(f"Null close prices: {nulls}")
    assert nulls == 0, "Found null close prices!"

    # Check 3: no negative prices
    cur.execute("SELECT COUNT(*) FROM raw.stock_prices WHERE close < 0;")
    negatives = cur.fetchone()[0]
    assert negatives == 0, "Found negative prices — bad data!"

    print("All validation checks passed.")
    cur.close()
    conn.close()

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='stock_price_pipeline',
    default_args=default_args,
    description='Fetch daily stock prices from Polygon.io into PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stocks', 'finance'],
) as dag:

    t1 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    t2 = PythonOperator(
        task_id='fetch_and_load',
        python_callable=fetch_and_load,
    )

    t3 = PythonOperator(
        task_id='validate',
        python_callable=validate,
    )

    # This defines the order: create → fetch → validate
    t1 >> t2 >> t3