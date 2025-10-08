create table if not exists <CATALOG>.<SCHEMA>.dim_product (
    product_key bigint not null primary key 
    , product_id bigint not null
    , product_category_id bigint
    , product_sub_category_id bigint
    , product_name string
    , product_color string
    , product_size string
    , sub_category_name string
    , category_name string
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['days(updated_at)']
);