create table if not exists iceberg.gold.dim_order (
    sales_order_key bigint   --sales_order_detail_id :V
    , sales_order_id bigint 
    , sales_order_detail_id bigint
    , product_id bigint
    , customer_id bigint
    , order_date timestamp
    , ship_date timestamp
    , sales_order_number varchar
    , order_month int 
    , order_year int
    , is_current BOOLEAN 
    , active_start timestamp 
    , active_end timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(active_start)']
);