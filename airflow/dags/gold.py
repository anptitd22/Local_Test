from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable

default_args = {
    "owner": "TEST",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SQL_BASE_DIR = "/opt/airflow/src/gold/scd1"

with DAG(
    dag_id="gold",
    default_args=default_args,
    description="gold for e-commerce data",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    catchup=True,
    tags=["gold"],
    max_active_runs=1,
    max_active_tasks=4,
    template_searchpath=[SQL_BASE_DIR],     
) as dag:

    # ----- CREATE STAGING -----
    with TaskGroup(group_id="create_staging") as create_staging_group:
        stg_customer_task = SQLExecuteQueryOperator(
            task_id="stg_customer",
            conn_id="trino_conn",
            sql="ddl/staging/stg_customer.sql",
            split_statements=True,
        )
        stg_product_task = SQLExecuteQueryOperator(
            task_id="stg_product",
            conn_id="trino_conn",
            sql="ddl/staging/stg_product.sql",
            split_statements=True,
        )
        stg_order_header_task = SQLExecuteQueryOperator(
            task_id="stg_order_header",
            conn_id="trino_conn",
            sql="ddl/staging/stg_order_header.sql",
            split_statements=True,
        )
        stg_order_detail_task = SQLExecuteQueryOperator(
            task_id="stg_order_detail",
            conn_id="trino_conn",
            sql="ddl/staging/stg_order_detail.sql",
            split_statements=True,
        )
        stg_product_category_task = SQLExecuteQueryOperator(
            task_id="stg_product_category",
            conn_id="trino_conn",
            sql="ddl/staging/stg_product_category.sql",
            split_statements=True,
        )
        stg_product_sub_category_task = SQLExecuteQueryOperator(
            task_id="stg_product_sub_category",
            conn_id="trino_conn",
            sql="ddl/staging/stg_product_sub_category.sql",
            split_statements=True,
        )

    # ----- ETL STAGING -----
    with TaskGroup(group_id="etl_staging") as etl_staging_group:
        etl_stg_customer_task = SQLExecuteQueryOperator(
            task_id="etl_stg_customer",
            conn_id="trino_conn",
            sql="etl/staging/etl_stg_customer.sql",
            split_statements=True,
        )
        etl_stg_product_task = SQLExecuteQueryOperator(
            task_id="etl_stg_product",
            conn_id="trino_conn",
            sql="etl/staging/etl_stg_product.sql",
            split_statements=True,
        )
        etl_stg_order_header_task = SQLExecuteQueryOperator(
            task_id="etl_stg_order_header",
            conn_id="trino_conn",
            sql="etl/staging/etl_stg_order_header.sql",
            split_statements=True,
        )
        etl_stg_order_detail_task = SQLExecuteQueryOperator(
            task_id="etl_stg_order_detail",
            conn_id="trino_conn",
            sql="etl/staging/etl_stg_order_detail.sql",
            split_statements=True,
        )
        etl_stg_product_category_task = SQLExecuteQueryOperator(
            task_id="etl_stg_product_category",
            conn_id="trino_conn",
            sql="etl/staging/etl_stg_product_category.sql",
            split_statements=True,
        )
        etl_stg_product_sub_category_task = SQLExecuteQueryOperator(
            task_id="etl_stg_product_sub_category",
            conn_id="trino_conn",
            sql="etl/staging/etl_stg_product_sub_category.sql",
            split_statements=True,
        )

        etl_stg_customer_task >> etl_stg_product_task >> etl_stg_order_header_task \
            >> etl_stg_order_detail_task >> etl_stg_product_category_task >> etl_stg_product_sub_category_task

    # ----- CREATE DIMENSION -----
    with TaskGroup(group_id="create_dimension") as create_dimension_group:
        dim_customer_task = SQLExecuteQueryOperator(
            task_id="dim_customer",
            conn_id="trino_conn",
            sql="ddl/dim_customer.sql",
            split_statements=True,
        )
        dim_product_task = SQLExecuteQueryOperator(
            task_id="dim_product",
            conn_id="trino_conn",
            sql="ddl/dim_product.sql",
            split_statements=True,
        )
        dim_order_task = SQLExecuteQueryOperator(
            task_id="dim_order",
            conn_id="trino_conn",
            sql="ddl/dim_order.sql",
            split_statements=True,
        )
        dim_date_task = SQLExecuteQueryOperator(
            task_id="dim_date",
            conn_id="trino_conn",
            sql="ddl/dim_date.sql",
            split_statements=True,
        )

    # ----- ETL DIMENSION -----
    with TaskGroup(group_id="etl_dimension") as etl_dimension_group:
        etl_dim_customer_task = SQLExecuteQueryOperator(
            task_id="etl_dim_customer",
            conn_id="trino_conn",
            sql="etl/etl_dim_customer.sql",
            split_statements=True,
        )
        etl_dim_product_task = SQLExecuteQueryOperator(
            task_id="etl_dim_product",
            conn_id="trino_conn",
            sql="etl/etl_dim_product.sql",
            split_statements=True,
        )
        etl_dim_order_task = SQLExecuteQueryOperator(
            task_id="etl_dim_order",
            conn_id="trino_conn",
            sql="etl/etl_dim_order.sql",
            split_statements=True,
        )
        etl_dim_date_task = SQLExecuteQueryOperator(
            task_id="etl_dim_date",
            conn_id="trino_conn",
            sql="etl/etl_dim_date.sql",
            split_statements=True,
        )

        etl_dim_customer_task >> etl_dim_product_task >> etl_dim_order_task >> etl_dim_date_task

    # ----- CREATE FACT -----
    with TaskGroup(group_id="create_fact") as create_fact_group:
        fact_sales_task = SQLExecuteQueryOperator(
            task_id="fact_sales",
            conn_id="trino_conn",
            sql="ddl/fact_sales.sql",
            split_statements=True,
        )
        fact_sales_month_task = SQLExecuteQueryOperator(
            task_id="fact_sales_month",
            conn_id="trino_conn",
            sql="ddl/fact_sales_month.sql",
            split_statements=True,
        )

    # ----- ETL FACT -----
    with TaskGroup(group_id="etl_fact") as etl_fact_group:
        etl_fact_sales_task = SQLExecuteQueryOperator(
            task_id="etl_fact_sales",
            conn_id="trino_conn",
            sql="etl/etl_fact_sales.sql",
            split_statements=True,
        )
        etl_fact_sales_month_task = SQLExecuteQueryOperator(
            task_id="etl_fact_sales_month",
            conn_id="trino_conn",
            sql="etl/etl_fact_sales_month.sql",
            split_statements=True,
        )

        etl_fact_sales_task >> etl_fact_sales_month_task

    create_staging_group >> etl_staging_group >> create_dimension_group \
        >> etl_dimension_group >> create_fact_group >> etl_fact_group
