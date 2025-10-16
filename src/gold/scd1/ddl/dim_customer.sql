create table if not exists iceberg.gold.dim_customer (
    customer_key bigint 
    , customer_id bigint 
    , account_number VARCHAR
    , middle_name VARCHAR
    , first_name VARCHAR
    , last_name VARCHAR
    , full_name VARCHAR
    , is_current BOOLEAN 
    , created_at TIMESTAMP
    , updated_at TIMESTAMP
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);