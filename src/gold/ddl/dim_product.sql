create table if not exists iceberg.gold.dim_product (
    product_key bigint 
    , product_id bigint 
    , product_category_id bigint
    , product_sub_category_id bigint
    , product_name varchar
    , product_color varchar
    , product_size varchar
    , sub_category_name varchar
    , category_name varchar
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);