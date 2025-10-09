INSERT INTO iceberg.silver.stg_order_detail
(
    sales_order_id  
    , sales_order_detail_id  
    , product_id 
    , order_qty 
    , unit_price
    , unit_price_discount
)
WITH order_detail__source AS (
    SELECT * FROM iceberg.silver.order_details
)
, order_detail__rename_column AS (
    SELECT
        salesorderid as sales_order_id  
        , salesorderdetailid as sales_order_detail_id  
        , productid as product_id 
        , orderqty as order_qty 
        , unitprice as unit_price
        , unitpricediscount as unit_price_discount
    FROM order_detail__source    
)
, order_detail__cast_type AS (
    SELECT
        CAST(sales_order_id AS BIGINT) AS sales_order_id
        , CAST(sales_order_detail_id AS BIGINT) AS sales_order_detail_id
        , CAST(product_id AS BIGINT) AS product_id
        , CAST(order_qty AS BIGINT) AS order_qty
        , CAST(unit_price AS DECIMAL(18,4)) AS unit_price
        , CAST(unit_price_discount AS DECIMAL(18,4)) AS unit_price_discount
    FROM order_detail__rename_column
)
, order_detail__add_fault_record AS (
    SELECT
        sales_order_id
        , sales_order_detail_id
        , product_id
        , order_qty
        , unit_price
        , unit_price_discount
    FROM order_detail__cast_type
    UNION ALL
    SELECT
        0 AS sales_order_id
        , 0 AS sales_order_detail_id
        , 0 AS product_id
        , 0 AS order_qty
        , 0.0000 AS unit_price
        , 0.0000 AS unit_price_discount
    UNION ALL
    SELECT
        -1 AS sales_order_id
        , -1 AS sales_order_detail_id
        , -1 AS product_id
        , 0 AS order_qty
        , 0.0000 AS unit_price
        , 0.0000 AS unit_price_discount
)
SELECT
    sales_order_id
    , COALESCE(sales_order_detail_id, -1) AS sales_order_detail_id
    , COALESCE(product_id, -1) AS product_id
    , COALESCE(order_qty, -1) AS order_qty
FROM order_detail__add_fault_record