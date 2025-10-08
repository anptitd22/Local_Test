INSERT INTO <CATALOG>.<SCHEMA>.dim_order
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
    , od.salesorderdetailid as sales_order_detail_id
    , od.productid as product_id
    , oh.customerid as customer_id
    , oh.orderdate as order_date
    , oh.shipdate as ship_date
    , oh.salesordernumber as sales_order_number
    , month(oh.orderdate) as order_month
    , year(oh.orderdate) as order_year
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM <CATALOG>.<SCHEMA>.order_detail od
JOIN <CATALOG>.<SCHEMA>.order_header oh
ON od.salesorderid = oh.salesorderid;