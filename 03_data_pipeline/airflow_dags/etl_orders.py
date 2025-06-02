from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def extract():
    with open("/path/to/orders_received.json") as f:
        return [json.loads(line) for line in f]

def transform(orders):
    return [o for o in orders if o["total_price"] > 0]

def load(orders):
    import psycopg2
    conn = psycopg2.connect("dbname=qcommerce user=admin")
    cur = conn.cursor()
    for o in orders:
        cur.execute("INSERT INTO orders (...) VALUES (...)", (...))
    conn.commit()

with DAG("etl_orders", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task