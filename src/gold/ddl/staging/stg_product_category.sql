create table if not exists iceberg.silver.stg_product_category (
    product_category_id bigint 
    , name varchar
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);