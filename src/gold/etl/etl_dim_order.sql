INSERT INTO iceberg.silver.dim_order
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
    , created_at
    , updated_at
)
SELECT
    od.sales_order_detail_id as sales_order_key
    , od.sales_order_id as sales_order_id
    , od.sales_order_detail_id as sales_order_detail_id
    , od.product_id as product_id
    , oh.customer_id as customer_id
    , oh.order_date as order_date
    , oh.ship_date as ship_date
    , oh.sales_order_number as sales_order_number
    , month(oh.orderdate) as order_month
    , year(oh.orderdate) as order_year
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.stg_order_detail od
JOIN iceberg.silver.stg_order_header oh
ON od.sales_order_id = oh.sales_order_id;