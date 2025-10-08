INSERT INTO iceberg.silver.dim_customer
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
    customerid as customer_key
    , customerid as customer_id
    , accountnumber as account_number
    , firstname as first_name
    , middlename as middle_name
    , lastname as last_name
    , concat_ws(' ', firstname, middlename, lastname) AS full_name
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.customer;