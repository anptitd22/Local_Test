DELETE FROM iceberg.gold.stg_order_header
WHERE updated_at >= timestamp '{{data_interval_start}}' and updated_at < timestamp '{{data_interval_end}}';

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
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS created_at
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS updated_at
FROM iceberg.silver.order_headers
WHERE orderdate >= timestamp '{{data_interval_start}}' and orderdate < timestamp '{{data_interval_end}}';