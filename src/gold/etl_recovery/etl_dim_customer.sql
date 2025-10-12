INSERT INTO iceberg.gold.dim_customer
(
    customer_key
    , customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , full_name
    , created_at
    , updated_at
    )
SELECT
    customer_id as customer_key
    , customer_id as customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , concat(first_name, ' ', middle_name, ' ', last_name) as full_name
    , current_timestamp as created_at
    , current_timestamp as updated_at
FROM iceberg.gold.stg_customer;