INSERT INTO iceberg.gold.stg_customer
(
    customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , created_at
    , updated_at
    )
SELECT
    CAST(customerid AS BIGINT) AS customer_id
    , CAST(accountnumber AS VARCHAR) AS account_number
    , CAST(firstname AS VARCHAR) AS first_name
    , CAST(middlename AS VARCHAR) AS middle_name
    , CAST(lastname AS VARCHAR) AS last_name
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.customers;