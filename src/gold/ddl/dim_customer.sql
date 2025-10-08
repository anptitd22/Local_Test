create table if not exists iceberg.silver.dim_customer (
    customer_key bigint 
    , customer_id bigint 
    , account_number VARCHAR
    , middle_name VARCHAR
    , first_name VARCHAR
    , last_name VARCHAR
    , full_name VARCHAR
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);