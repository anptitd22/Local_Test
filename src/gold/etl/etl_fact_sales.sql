INSERT INTO <CATALOG>.<SCHEMA>.dim_customer
(
    product_key
    , customer_key
    , sales_order_key
    , product_list_price
    , order_unit_price
    , order_unit_price_discount
    , order_sub_total_group
    , order_qty
    , created_at
    , updated_at
    )
SELECT
    p.productid as product_key
    , c.customerid as customer_key
    , od.salesorderid as sales_order_key
    , p.productlistprice as product_list_price
    , od.unitprice as order_unit_price
    , od.unitpricediscount as order_unit_price_discount
    , oh.subtotal as order_sub_total_group
    , od.orderqty as order_qty
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM <CATALOG>.<SCHEMA>.product p
JOIN <CATALOG>.<SCHEMA>.customer c
ON p.productid = c.customerid
JOIN <CATALOG>.<SCHEMA>.order_header oh
ON c.customerid = oh.customerid
JOIN <CATALOG>.<SCHEMA>.order_detail od
ON oh.salesorderid = od.salesorderid;    