create table if not exists iceberg.gold.stg_product_category (
    product_category_id bigint 
    , name varchar
    , created_at timestamp with time zone
    , updated_at timestamp with time zone
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);