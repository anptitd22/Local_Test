from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.bash import BashOperator

SCRIPT_DIR = "/opt/airflow/src/silver_v2"
DATA_DIR = "/opt/airflow/dataset/datamart"

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
    dag_id="silver",
    default_args=default_args,
    description="silver for e-commerce data",
    tags=["silver"],
    schedule="*/30 * * * *",  # chạy mỗi 30 phút
    catchup=False,
) as dag:
    with TaskGroup("data_silver") as data_silver_group:
        customer_task = BashOperator(
            task_id="upload_customer",
            bash_command=(
                f"python {SCRIPT_DIR}/upload_customer.py "
                f'--file "{DATA_DIR}/Customers.csv"'
            ),
        )

        product_task = BashOperator(
            task_id="upload_product",
            bash_command=(
                f"python {SCRIPT_DIR}/upload_product.py "
                f'--file "{DATA_DIR}/Product.csv"'
            ),
        )

        order_header_task = BashOperator(
            task_id="upload_order_header",
            bash_command=(
                f"python {SCRIPT_DIR}/upload_order_header.py "
                f'--file "{DATA_DIR}/OrderHeader.csv"'
            ),
        )

        order_detail_task = BashOperator(
            task_id="upload_order_detail",
            bash_command=(
                f"python {SCRIPT_DIR}/upload_order_detail.py "
                f'--file "{DATA_DIR}/OrderDetail.csv"'
            ),
        )

        product_category_task = BashOperator(
            task_id="upload_product_category",
            bash_command=(
                f"python {SCRIPT_DIR}/upload_product_category.py "
                f'--file "{DATA_DIR}/ProductCategory.csv"'
            ),
        )

        product_sub_category_task = BashOperator(
            task_id="upload_product_sub_category",
            bash_command=(
                f"python {SCRIPT_DIR}/upload_product_sub_category.py "
                f'--file "{DATA_DIR}/ProductSubCategory.csv"'
            ),
        )

        customer_task >> product_task >> order_header_task >> order_detail_task >> product_category_task >> product_sub_category_task
