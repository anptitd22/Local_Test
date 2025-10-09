from datetime import datetime, timedelta
from airflow.providers.standard.operators.empty import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG

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
    with TaskGroup("create_staging") as create_staging_group:
        stg_customer_task = BashOperator(
            task_id='stg_customer',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/ddl/staging/stg_customer.sql",
        )
        stg_product_task = BashOperator(
            task_id='stg_product',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/ddl/staging/stg_product.sql",
        )
        stg_order_header_task = BashOperator(
            task_id='stg_order_header',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/ddl/staging/stg_order_header.sql",
        )
        stg_order_detail_task = BashOperator(
            task_id='stg_order_detail',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/ddl/staging/stg_order_detail.sql",
        )
        stg_product_category_task = BashOperator(
            task_id='stg_product_category',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/ddl/staging/stg_product_category.sql",
        )
        stg_product_sub_category_task = BashOperator(
            task_id='stg_product_sub_category',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/ddl/staging/stg_product_sub_category.sql",
        )
        stg_customer_task >> stg_product_task >> stg_order_header_task >> stg_order_detail_task >> stg_product_category_task >> stg_product_sub_category_task
    
    with TaskGroup("etl_staging") as etl_staging_group:
        etl_stg_customer_task = BashOperator(
            task_id='etl_stg_customer',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/etl/staging/etl_stg_customer.sql",
        )
        etl_stg_product_task = BashOperator(
            task_id='etl_stg_product',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/etl/staging/etl_stg_product.sql",
        )
        etl_stg_order_header_task = BashOperator(
            task_id='etl_stg_order_header',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/etl/staging/etl_stg_order_header.sql",
        )
        etl_stg_order_detail_task = BashOperator(
            task_id='etl_stg_order_detail',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/etl/staging/etl_stg_order_detail.sql",
        )
        etl_stg_product_category_task = BashOperator(
            task_id='etl_stg_product_category',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/etl/staging/etl_stg_product_category.sql",
        )
        etl_stg_product_sub_category_task = BashOperator(
            task_id='etl_stg_product_sub_category',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema silver --file /tmp/gold/etl/staging/etl_stg_product_sub_category.sql",
        )
        etl_stg_customer_task >> etl_stg_product_task >> etl_stg_order_header_task >> etl_stg_order_detail_task >> etl_stg_product_category_task >> etl_stg_product_sub_category_task
    
    with TaskGroup("create_dimension") as create_dimension_group:
        dim_customer_task = BashOperator(
            task_id='dim_customer',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/ddl/dim_customer.sql",
        )
        dim_product_task = BashOperator(
            task_id='dim_product',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/ddl/dim_product.sql",
        )
        dim_order_task = BashOperator(
            task_id='dim_order',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/ddl/dim_order.sql",
        )
        dim_date_task = BashOperator(
            task_id='dim_date',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/ddl/dim_date.sql",
        )
        dim_customer_task >> dim_product_task >> dim_order_task >> dim_date_task
    
    with TaskGroup("etl_dimension") as etl_dimension_group:
        etl_dim_customer_task = BashOperator(
            task_id='etl_dim_customer',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/etl/etl_dim_customer.sql",
        )
        etl_dim_product_task = BashOperator(
            task_id='etl_dim_product',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/etl/etl_dim_product.sql",
        )
        etl_dim_order_task = BashOperator(
            task_id='etl_dim_order',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/etl/etl_dim_order.sql",
        )
        etl_dim_date_task = BashOperator(
            task_id='etl_dim_date',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/etl/etl_dim_date.sql",
        )
        etl_dim_customer_task >> etl_dim_product_task >> etl_dim_order_task >> etl_dim_date_task
    
    with TaskGroup("create_fact") as create_fact_group:
        fact_sales_task = BashOperator(
            task_id='fact_sales',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/ddl/fact_sales.sql",
        )
    with TaskGroup("etl_fact") as etl_fact_group:
        etl_fact_sales_task = BashOperator(
            task_id='etl_fact_sales',
            bash_command="docker exec -it trino trino --server http://trino:8080 --catalog iceberg --schema gold --file /tmp/gold/etl/etl_fact_sales.sql",
        )
    create_staging_group >> etl_staging_group >> create_dimension_group >> etl_dimension_group >> create_fact_group >> etl_fact_group
