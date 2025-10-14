DELETE FROM iceberg.gold.stg_order_header
WHERE date(updated_at) = current_date;

INSERT INTO iceberg.gold.stg_order_header
(
    sales_order_id  
    , order_date
    , ship_date
    , sales_order_number
    , customer_id  
    , sub_total
    , created_at
    , updated_at
)
SELECT
    CAST(salesorderid AS BIGINT) AS sales_order_id
    , CAST(orderdate AS TIMESTAMP) AS order_date
    , CAST(shipdate AS TIMESTAMP) AS ship_date
    , CAST(salesordernumber AS VARCHAR) AS sales_order_number
    , CAST(customerid AS BIGINT) AS customer_id
    , CAST(subtotal AS DECIMAL(18,4)) AS sub_total
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.order_headers;
-- WHERE date(createdat) = current_date;