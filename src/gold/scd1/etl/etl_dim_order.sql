DELETE FROM iceberg.gold.dim_order
WHERE order_date >= timestamp '{{data_interval_start}}'
    AND order_date < timestamp '{{data_interval_end}}';

INSERT INTO iceberg.gold.dim_order (
    sales_order_key,
    sales_order_id,
    sales_order_detail_id,
    product_id,
    customer_id,
    order_date,
    ship_date,
    sales_order_number,
    order_month,
    order_year,
    created_at,
    updated_at
)
SELECT
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(od.sales_order_detail_id as varchar) || ':' ||
                cast(date(oh.updated_at) as varchar)
            )
        )
    )) as sales_order_key,
    od.sales_order_id,
    od.sales_order_detail_id,
    od.product_id,
    oh.customer_id,
    oh.order_date,
    oh.ship_date,
    oh.sales_order_number,
    month(oh.order_date) as order_month,
    year(oh.order_date) as order_year,
    CAST('{{data_interval_start}}' AS TIMESTAMP) as created_at,
    CAST('{{data_interval_start}}' AS TIMESTAMP) as updated_at
FROM iceberg.gold.stg_order_detail od
RIGHT JOIN iceberg.gold.stg_order_header oh
    ON od.sales_order_id = oh.sales_order_id
    AND oh.order_date >= timestamp '{{data_interval_start}}'
    AND oh.order_date < timestamp '{{data_interval_end}}';