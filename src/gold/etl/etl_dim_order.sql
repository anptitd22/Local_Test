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
    od.salesorderid as sales_order_key
    , od.salesorderid as sales_order_id
    , sales_order_detail_id
    , product_id
    , customer_id
    , order_date
    , ship_date
    , sales_order_number
    , month(oh.orderdate) as order_month
    , year(oh.orderdate) as order_year
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.stg_order_detail od
JOIN iceberg.silver.stg_order_header oh
ON od.sales_order_id = oh.sales_order_id;