DELETE FROM iceberg.gold.stg_customer 
WHERE updated_at >= timestamp '{{data_interval_start}}' and updated_at < timestamp '{{data_interval_end}}';

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
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS created_at
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS updated_at
FROM iceberg.silver.customers;
-- WHERE date(createdat) = current_date;