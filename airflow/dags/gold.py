from datetime import datetime, timedelta
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from src.silver_v2 import upload_customer, upload_product, upload_order_header, upload_order_detail, upload_product_category, upload_product_sub_category

default_args = {
    'owner': 'TEST',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'silver',
    default_args=default_args,
    description='silver for e-commerce data',
    tags=['silver'],
    schedule='*/30 * * * *',  #'0 10 * * *' Chạy hàng ngày 10h
    catchup=False
) as dag:
    with TaskGroup("data_silver") as data_silver_group:
        customer_task = PythonOperator(
            task_id='upload_customer',
            python_callable=upload_customer,
        )
        product_task = PythonOperator(
            task_id='upload_product',
            python_callable=upload_product,
        )
        order_header_task = PythonOperator(
            task_id='upload_order_header',
            python_callable=upload_order_header,
        )
        order_detail_task = PythonOperator(
            task_id='upload_order_detail',
            python_callable=upload_order_detail,
        )
        product_category_task = PythonOperator(
            task_id='upload_product_category',
            python_callable=upload_product_category,
        )
        product_sub_category_task = PythonOperator(
            task_id='upload_product_sub_category',
            python_callable=upload_product_sub_category,
        )
        customer_task >> product_task >> order_header_task >> order_detail_task >> product_category_task >> product_sub_category_task