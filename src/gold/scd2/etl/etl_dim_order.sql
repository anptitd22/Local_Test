UPDATE iceberg.gold.dim_order 
SET is_current = false,
    active_end = current_timestamp
WHERE is_current = true 
  AND active_start >= timestamp '{{data_interval_start}}' 
  and active_start < timestamp '{{data_interval_end}}'
  AND sales_order_detail_id IN (
    SELECT DISTINCT od.sales_order_detail_id 
    FROM iceberg.gold.stg_order_detail od
    WHERE od.updated_at >= timestamp '{{data_interval_start}}' 
    and od.updated_at < timestamp '{{data_interval_end}}'
  );

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
    is_current,
    active_start,
    active_end
)
SELECT
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(od.sales_order_detail_id as varchar) || ':' ||
                cast(date(od.updated_at) as varchar)
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
    TRUE as is_current,
    od.updated_at as active_start,
    TIMESTAMP '9999-12-31' as active_end
FROM iceberg.gold.stg_order_detail od
JOIN iceberg.gold.stg_order_header oh
    ON od.sales_order_id = oh.sales_order_id 
    AND oh.order_date >= timestamp '{{data_interval_start}}'
    AND oh.order_date < timestamp '{{data_interval_end}}'
WHERE od.updated_at >= timestamp '{{data_interval_start}}' 
    and od.updated_at < timestamp '{{data_interval_end}}';