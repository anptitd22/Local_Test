create table if not exists iceberg.gold.fact_sales (
    sales_key bigint
    , product_key bigint 
    , customer_key bigint 
    , sales_order_key bigint 
    , date_key bigint
    , order_unit_price decimal(18,4)
    , order_unit_price_discount decimal(18,4)
    , order_unit_sub_total decimal(18,4)   --order_unit_price * order_qty - order_unit_price_discount :V
    , order_qty bigint
    , created_at timestamp
    , updated_at timestamp 
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);