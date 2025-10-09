create table if not exists iceberg.gold.fact_sales_month (
    sales_month_key bigint
    , customer_key bigint 
    , month_sub_total decimal(18,4)
    , month bigint
    , year bigint
)
WITH (
    format = 'PARQUET'
);