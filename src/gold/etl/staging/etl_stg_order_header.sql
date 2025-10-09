INSERT INTO iceberg.silver.stg_order_header
(
    sales_order_id  
    , order_date
    , ship_date
    , sales_order_number
    , customer_id  
    , sub_total
)
WITH order_header__source AS (
    SELECT * FROM iceberg.silver.order_headers
)
, order_header__rename_column AS (
    SELECT
        salesorderid as sales_order_id  
        , orderdate as order_date
        , shipdate as ship_date
        , salesordernumber as sales_order_number
        , customerid as customer_id  
        , subtotal as sub_total
    FROM order_header__source    
)
, order_header__cast_type AS (
    SELECT
        CAST(sales_order_id AS BIGINT) AS sales_order_id
        , CAST(order_date AS TIMESTAMP) AS order_date
        , CAST(ship_date AS TIMESTAMP) AS ship_date
        , CAST(sales_order_number AS VARCHAR) AS sales_order_number
        , CAST(customer_id AS BIGINT) AS customer_id
        , CAST(sub_total AS DECIMAL(18,4)) AS sub_total
    FROM order_header__rename_column
)
, order_header__add_fault_record AS (
    SELECT
        sales_order_id
        , order_date
        , ship_date
        , sales_order_number
        , customer_id
        , sub_total
    FROM order_header__cast_type
    UNION ALL
    SELECT
        0 AS sales_order_id
        , CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS order_date
        , CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS ship_date
        , 'undefined' AS sales_order_number
        , 0 AS customer_id  
        , 0.0000 AS sub_total
    UNION ALL
    SELECT
        -1 AS sales_order_id
        , CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS order_date
        , CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS ship_date
        , 'undefined' AS sales_order_number
        , -1 AS customer_id 
        , 0.0000 AS sub_total 
)
SELECT
    sales_order_id
    , COALESCE(order_date, CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AS order_date
    , COALESCE(ship_date, CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AS ship_date
    , COALESCE(sales_order_number, 'undefined') AS sales_order_number
    , COALESCE(customer_id, -1) AS customer_id  
    , COALESCE(sub_total, 0.0000) AS sub_total
FROM order_header__add_fault_record;