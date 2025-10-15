create table if not exists iceberg.gold.stg_product (
    product_id bigint
    , name varchar
    , color varchar
    , list_price decimal(18,4)
    , size varchar
    , product_sub_category_id bigint
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);