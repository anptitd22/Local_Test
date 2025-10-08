create table if not exists iceberg.silver.fact_sales (
    sales_key bigint
    , product_key bigint 
    , customer_key bigint 
    , sales_order_key bigint 
    , date_key bigint
    , product_list_price decimal(18,4)
    , order_unit_price decimal(18,4)
    , order_unit_price_discount decimal(18,4)
    , order_sub_total_group decimal(18,4)
    , order_qty bigint
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);