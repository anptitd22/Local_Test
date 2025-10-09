INSERT INTO iceberg.silver.stg_customer
(
    customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , full_name
    , created_at
    , updated_at
    )
WITH customer__source AS (
    SELECT * FROM iceberg.silver.customers
)

, customer__rename_column AS (
    SELECT
        customerid as customer_id
        , accountnumber as account_number
        , firstname as first_name
        , middlename as middle_name
        , lastname as last_name
    FROM customer__source    
)

, customer__cast_type AS (
    SELECT
        CAST(customer_key AS BIGINT) AS customer_key
        , CAST(customer_id AS BIGINT) AS customer_id
        , CAST(account_number AS VARCHAR) AS account_number
        , CAST(first_name AS VARCHAR) AS first_name
        , CAST(middle_name AS VARCHAR) AS middle_name
        , CAST(last_name AS VARCHAR) AS last_name
    FROM customer__rename_column
)

, customer__calculate_measure AS (
    SELECT
        customer_id
        , account_number
        , first_name
        , middle_name
        , last_name
        , concat_ws(' ', first_name, middle_name, last_name) AS full_name
    FROM customer__cast_type
)

, customer__add_fault_record AS (
    SELECT
        customer_key
        , customer_id
        , account_number
        , first_name
        , middle_name
        , last_name
        , full_name
    FROM customer__calculate_measure
    UNION ALL
    SELECT
        CAST(0 AS BIGINT) AS customer_id
        , CAST('undefined' AS VARCHAR) AS account_number
        , CAST('undefined' AS VARCHAR) AS first_name
        , CAST('undefined' AS VARCHAR) AS middle_name
        , CAST('undefined' AS VARCHAR) AS last_name
        , CAST('undefined' AS VARCHAR) AS full_name
    UNION ALL
    SELECT
        CAST(-1 AS BIGINT) AS customer_id
        , CAST('unknown' AS VARCHAR) AS account_number
        , CAST('unknown' AS VARCHAR) AS first_name
        , CAST('unknown' AS VARCHAR) AS middle_name
        , CAST('unknown' AS VARCHAR) AS last_name
        , CAST('unknown' AS VARCHAR) AS full_name
)

SELECT
    customer_id
    , COALESCE (account_number, 'undefined') AS account_number
    , COALESCE (first_name, 'undefined') AS first_name
    , COALESCE (middle_name, 'undefined') AS middle_name
    , COALESCE (last_name, 'undefined') AS last_name
    , COALESCE (full_name, 'undefined') AS full_name
    , current_timestamp as created_at
    , current_timestamp as updated_at
FROM customer__add_fault_record;