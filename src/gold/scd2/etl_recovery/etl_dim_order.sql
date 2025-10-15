TRUNCATE TABLE iceberg.gold.dim_order;

INSERT INTO iceberg.gold.dim_order
(
    sales_order_key
    , sales_order_id
    , sales_order_detail_id
    , product_id
    , customer_id
    , order_date
    , ship_date
    , sales_order_number
    , order_month
    , order_year
    , is_current  
    , active_start  
    , active_end  
)
SELECT
    ABS(from_big_endian_64(
            xxhash64(
                to_utf8(
                    cast(od.sales_order_detail_id as varchar) || ':' ||
                    cast(date(od.updated_at) as varchar)
                )
            )
        )) as sales_order_key
    , od.sales_order_id as sales_order_id
    , od.sales_order_detail_id as sales_order_detail_id
    , od.product_id as product_id
    , oh.customer_id as customer_id
    , oh.order_date as order_date
    , oh.ship_date as ship_date
    , oh.sales_order_number as sales_order_number
    , month(oh.order_date) as order_month
    , year(oh.order_date) as order_year
    , TRUE as is_current
    , od.updated_at as active_start
    , TIMESTAMP '9999-12-31' as active_end
FROM iceberg.gold.stg_order_detail od
LEFT JOIN iceberg.gold.stg_order_header oh
ON od.sales_order_id = oh.sales_order_id;