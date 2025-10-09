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
    , order_unit_sub_total
    , order_qty
    , created_at
    , updated_at
    )
SELECT
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(c.customer_id  as varchar) || ':' ||
                cast(p.product_id as varchar) || ':' ||
                cast(od.sales_order_id as varchar) || ':' ||
                cast(oh.order_date as varchar)
            )
        ) 
    )) AS sales_key
    , p.product_id as product_key
    , c.customer_id as customer_key
    , od.sales_order_id as sales_order_key
    , CAST(format_datetime(oh.order_date, 'yyyyMMd') AS BIGINT) * 1 
        + (CASE WHEN length(format_datetime(oh.order_date, 'yyyyMMdd')) = 7 THEN 0 ELSE 0 END) AS date_key
    , p.list_price as product_list_price
    , od.unit_price as order_unit_price
    , od.unit_price_discount as order_unit_price_discount
    , CAST((od.unit_price * od.order_qty) - od.unit_price_discount AS DECIMAL(18, 4)) as order_unit_sub_total
    , od.order_qty as order_qty
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.stg_order_detail od
LEFT JOIN iceberg.silver.stg_product p
ON p.product_id = od.product_id
LEFT JOIN iceberg.silver.stg_order_header oh
ON od.sales_order_id = oh.sales_order_id
LEFT JOIN iceberg.silver.stg_customer c
ON oh.customer_id = c.customer_id;