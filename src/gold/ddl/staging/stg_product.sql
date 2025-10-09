create table if not exists iceberg.silver.stg_product (
    product_id bigint
    , name varchar
    , color varchar
    , list_price decimal(18,4)
    , size varchar
    , product_sub_category_id bigint
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);