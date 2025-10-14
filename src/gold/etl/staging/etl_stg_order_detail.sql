DELETE FROM iceberg.gold.stg_order_detail
WHERE date(updated_at) = current_date;

INSERT INTO iceberg.gold.stg_order_detail
(
    sales_order_id  
    , sales_order_detail_id  
    , product_id 
    , order_qty 
    , unit_price
    , unit_price_discount
    , created_at
    , updated_at
)
SELECT
    CAST(salesorderid AS BIGINT) AS sales_order_id
    , CAST(salesorderdetailid AS BIGINT) AS sales_order_detail_id
    , CAST(productid AS BIGINT) AS product_id
    , CAST(orderqty AS BIGINT) AS order_qty
    , CAST(unitprice AS DECIMAL(18,4)) AS unit_price
    , CAST(unitpricediscount AS DECIMAL(18,4)) AS unit_price_discount
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.order_details;
-- WHERE date(createdat) = current_date;
