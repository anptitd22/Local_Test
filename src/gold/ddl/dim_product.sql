create table if not exists <CATALOG>.<SCHEMA>.dim_product (
    product_key bigint not null primary key generated always as identity (start with 1 increment by 1)
    , product_id bigint not null
    , product_name string
    , product_color string
    , product_list_price decimal(18,4)
    , product_size string
    , product_sub_category bigint
    , product_category_id bigint
    , sub_category_name string
    , category_name string
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['days(updated_at)']
);