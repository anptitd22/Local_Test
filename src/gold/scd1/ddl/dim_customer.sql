create table if not exists iceberg.gold.dim_customer (
    customer_key bigint 
    , customer_id bigint 
    , account_number VARCHAR
    , middle_name VARCHAR
    , first_name VARCHAR
    , last_name VARCHAR
    , full_name VARCHAR
    , created_at TIMESTAMP WITH TIME ZONE
    , updated_at TIMESTAMP WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(updated_at)']
);