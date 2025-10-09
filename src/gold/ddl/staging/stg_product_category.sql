create table if not exists iceberg.silver.stg_product_category (
    product_category_id bigint 
    , name varchar
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);