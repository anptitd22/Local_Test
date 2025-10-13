create table if not exists iceberg.gold.dim_product (
    product_key bigint 
    , product_id bigint 
    , product_category_id bigint
    , product_sub_category_id bigint
    , product_list_price decimal(18,4)
    , product_name varchar
    , product_color varchar
    , product_size varchar
    , sub_category_name varchar
    , category_name varchar
    , is_current BOOLEAN 
    , active_start timestamp with time zone
    , active_end timestamp with time zone
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(active_start)']
);