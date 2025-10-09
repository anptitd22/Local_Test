create table if not exists iceberg.silver.stg_customer (
    customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , full_name
    , created_at
    , updated_at
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);