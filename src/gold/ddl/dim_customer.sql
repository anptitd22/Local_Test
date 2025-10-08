create table if not exists <CATALOG>.<SCHEMA>.dim_customer (
    customer_key bigint key generated always as identity (start with 1 increment by 1)
    , customer_id bigint not null
    , account_number string
    , customer_name string
    , middle_name string
    , first_name string
    , last_name string
    , full_name string
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['days(updated_at)']
);