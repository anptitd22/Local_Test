create table if not exists iceberg.gold.stg_order_detail (
    sales_order_id bigint
    , sales_order_detail_id bigint
    , product_id bigint
    , order_qty bigint
    , unit_price decimal(18,4)
    , unit_price_discount decimal(18,4)
    , created_at timestamp with time zone
    , updated_at timestamp with time zone
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);