create table if not exists iceberg.gold.stg_customer (
    customer_id bigint
    , account_number varchar
    , first_name varchar
    , middle_name varchar
    , last_name varchar
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);