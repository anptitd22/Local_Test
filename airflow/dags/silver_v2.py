import sys
import os
from datetime import datetime, timedelta

airflow_src_path = "/opt/airflow/src"
if airflow_src_path not in sys.path:
    sys.path.insert(0, airflow_src_path)

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from src.silver_v2.upload_silver import (
    upload_table_to_iceberg,
    TABLE_CONFIG
)

DATA_DIR = "/opt/airflow/dataset/datamart"

TABLE_TASKS_CONFIG = {
    'customers': {
        'task_id': 'upload_customer',
        'table_key': 'customers'
    },
    'products': {
        'task_id': 'upload_product', 
        'table_key': 'products'
    },
    'order_headers': {
        'task_id': 'upload_order_header',
        'table_key': 'order_headers'
    },
    'order_details': {
        'task_id': 'upload_order_detail',
        'table_key': 'order_details'
    },
    'product_categories': {
        'task_id': 'upload_product_category',
        'table_key': 'product_categories'
    },
    'product_sub_categories': {
        'task_id': 'upload_product_sub_category',
        'table_key': 'product_sub_categories'
    }
}

default_args = {
    "owner": "TEST",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_v2",
    default_args=default_args,
    description="silver for e-commerce data",
    tags=["silver"],
    schedule="*/30 * * * *",  # chạy mỗi 30 phút
    catchup=False,
) as dag:
    
    with TaskGroup("data_silver") as data_silver_group:

        table_order = [
            'customers',
            'products', 
            'order_headers',
            'order_details', 
            'product_categories',
            'product_sub_categories'
        ]
        
        tasks = {}
        
        for table_key in table_order:
            config = TABLE_TASKS_CONFIG[table_key]
            
            task = PythonOperator(
                task_id=config['task_id'],
                python_callable=upload_table_to_iceberg,
                op_kwargs={
                    'table_key': config['table_key'],
                    'data_dir': DATA_DIR,
                    'operation': 'upload',
                    'namespace': 'silver',
                    'namespace_etl': 'gold'
                }, 
            )
            
            tasks[table_key] = task
        
        for i in range(len(table_order) - 1):
            current_table = table_order[i]
            next_table = table_order[i + 1]
            tasks[current_table] >> tasks[next_table]