INSERT INTO iceberg.silver.fact_sales
(
    sales_key
    ,product_key
    , customer_key
    , sales_order_key
    , date_key
    , product_list_price
    , order_unit_price
    , order_unit_price_discount
    , order_sub_total_group
    , order_qty
    , created_at
    , updated_at
    )
SELECT
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(c.customerid  as varchar) || ':' ||
                cast(p.productid as varchar) || ':' ||
                cast(od.salesorderid as varchar) || ':' ||
                cast(oh.orderdate as varchar)
            )
        ) 
    )) AS sales_key
    , p.productid as product_key
    , c.customerid as customer_key
    , od.salesorderid as sales_order_key
    , CAST(format_datetime(oh.orderdate, 'yyyyMMd') AS BIGINT) * 1 
        + (CASE WHEN length(format_datetime(oh.orderdate, 'yyyyMMdd')) = 7 THEN 0 ELSE 0 END) AS date_key
    , p.listprice as product_list_price
    , od.unitprice as order_unit_price
    , od.unitpricediscount as order_unit_price_discount
    , oh.subtotal as order_sub_total_group
    , od.orderqty as order_qty
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.product p
JOIN iceberg.silver.order_detail od
ON p.productid = od.productid
JOIN iceberg.silver.order_header oh
ON od.salesorderid = oh.salesorderid
JOIN iceberg.silver.customer c
ON oh.customerid = c.customerid;