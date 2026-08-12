import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

DBT = "/opt/airflow/dbt_venv/bin"
DBT_PROJECT = "/opt/airflow/dbt/dbt_my"

COPY_TABLES = [
    ("OLIST_CUSTOMER_DATASET", "olist_customers_dataset.csv"),
    ("OLIST_GEOLOCATION_DATASET", "olist_geolocation_dataset.csv"),
    ("OLIST_ORDER_ITEMS_DATASET", "olist_order_items_dataset.csv"),
    ("OLIST_ORDER_PAYMENTS_DATASET", "olist_order_payments_dataset.csv"),
    ("OLIST_ORDER_REVIEWS_DATASET", "olist_order_reviews_dataset.csv"),
    ("OLIST_ORDERS_DATASET", "olist_orders_dataset.csv"),
    ("OLIST_PRODUCTS_DATASET", "olist_products_dataset.csv"),
    ("OLIST_SELLERS_DATASET", "olist_sellers_dataset.csv"),
]


def _reload_raw(**ctx):
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="TALABAT_WH",
        database="TALABAT",
        schema="RAW",
        role="DBT_ROLE",
    )
    cur = conn.cursor()
    try:
        for table, csv_file in COPY_TABLES:
            cur.execute(
                f"COPY INTO TALABAT.RAW.{table} "
                f"FROM @TALABAT.RAW.TALABAT_STAGE/{csv_file} "
                f"FILE_FORMAT = TALABAT.RAW.CSV_FMT "
                f"ON_ERROR = 'CONTINUE'"
            )
            result = cur.fetchall()
            print(f"COPY {table}: {result}")
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="talabat_batch",
    start_date=datetime(2026, 8, 8),
    schedule="@daily",
    catchup=False,
    tags=["talabat", "dbt", "snowflake"],
) as dag:

    reload_raw = PythonOperator(
        task_id="reload_raw",
        python_callable=_reload_raw,
    )

    dbt_build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=f"{DBT} build --exclude tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )

    reload_raw >> dbt_build_core
