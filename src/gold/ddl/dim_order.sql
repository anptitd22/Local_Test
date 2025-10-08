create table if not exists <CATALOG>.<SCHEMA>.dim_order (
    sales_order_key bigint not null primary key generated always as identity (start with 1 increment by 1)
    , sales_order_id bigint not null
    , sales_order_detail_id bigint
    , product_id bigint
    , customer_id bigint
    , order_date timestamp
    , ship_date timestamp
    , sales_order_number string
    , order_month int 
    , order_year int
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['days(updated_at)']
);