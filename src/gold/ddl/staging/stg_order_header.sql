create table if not exists iceberg.gold.stg_order_header (
    sales_order_id bigint 
    , order_date timestamp
    , ship_date timestamp
    , sales_order_number varchar
    , customer_id bigint
    , sub_total decimal(18,4)
    , created_at timestamp with time zone
    , updated_at timestamp with time zone
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);