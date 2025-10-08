create table if not exists iceberg.silver.dim_product (
    product_key bigint 
    , product_id bigint 
    , product_category_id bigint
    , product_sub_category_id bigint
    , product_name VARCHAR
    , product_color VARCHAR
    , product_size VARCHAR
    , sub_category_name VARCHAR
    , category_name VARCHAR
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);