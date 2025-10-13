create table if not exists iceberg.gold.dim_customer (
    customer_key bigint 
    , customer_id bigint 
    , account_number VARCHAR
    , middle_name VARCHAR
    , first_name VARCHAR
    , last_name VARCHAR
    , full_name VARCHAR
    , is_current BOOLEAN 
    , active_start timestamp with time zone 
    , active_end timestamp with time zone
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(active_start)']
);