-- UPDATE iceberg.gold.dim_order 
-- SET is_current = false,
--     active_end = current_timestamp
-- WHERE is_current = true 
--     and active_start < timestamp '{{data_interval_end}}'
--     AND sales_order_detail_id IN (
--         SELECT DISTINCT od.sales_order_detail_id 
--         FROM iceberg.gold.stg_order_detail od
--         WHERE od.updated_at >= timestamp '{{data_interval_start}}' 
--         and od.updated_at < timestamp '{{data_interval_end}}'
--     );

-- INSERT INTO iceberg.gold.dim_order (
--     sales_order_key,
--     sales_order_id,
--     sales_order_detail_id,
--     product_id,
--     customer_id,
--     order_date,
--     ship_date,
--     sales_order_number,
--     order_month,
--     order_year,
--     is_current,
--     active_start,
--     active_end
-- )
-- SELECT
--     ABS(from_big_endian_64(
--         xxhash64(
--             to_utf8(
--                 cast(od.sales_order_detail_id as varchar) || ':' ||
--                 cast(date(od.updated_at) as varchar)
--             )
--         )
--     )) as sales_order_key,
--     od.sales_order_id,
--     od.sales_order_detail_id,
--     od.product_id,
--     oh.customer_id,
--     oh.order_date,
--     oh.ship_date,
--     oh.sales_order_number,
--     month(oh.order_date) as order_month,
--     year(oh.order_date) as order_year,
--     TRUE as is_current,
--     od.updated_at as active_start,
--     TIMESTAMP '9999-12-31' as active_end
-- FROM iceberg.gold.stg_order_detail od
-- JOIN iceberg.gold.stg_order_header oh
--     ON od.sales_order_id = oh.sales_order_id 
--     AND oh.order_date >= timestamp '{{data_interval_start}}'
--     AND oh.order_date < timestamp '{{data_interval_end}}'
-- WHERE od.updated_at >= timestamp '{{data_interval_start}}' 
--     and od.updated_at < timestamp '{{data_interval_end}}';

WITH stg_order__source AS (
    SELECT
        od.sales_order_id,
        od.sales_order_detail_id,
        od.product_id,
        od.updated_at,
        oh.customer_id,
        oh.order_date,
        oh.ship_date,
        oh.sales_order_number
    FROM iceberg.gold.stg_order_detail od
    JOIN iceberg.gold.stg_order_header oh
        ON od.sales_order_id = oh.sales_order_id 
        AND oh.order_date >= timestamp '{{data_interval_start}}'
        AND oh.order_date < timestamp '{{data_interval_end}}'
    WHERE od.updated_at >= timestamp '{{data_interval_start}}' 
        AND od.updated_at < timestamp '{{data_interval_end}}'
),
record_changed_local AS (
    SELECT 
        s.sales_order_id,
        s.sales_order_detail_id,
        s.product_id,
        s.customer_id,
        s.order_date,
        s.ship_date,
        s.sales_order_number,
        s.updated_at
    FROM iceberg.gold.dim_order d
    JOIN stg_order__source s
        ON s.sales_order_detail_id = d.sales_order_detail_id
        AND d.updated_at < timestamp '{{data_interval_end}}'
    WHERE
        COALESCE(d.sales_order_id, -1) <> COALESCE(s.sales_order_id, -1)
        OR COALESCE(d.product_id, -1) <> COALESCE(s.product_id, -1)
        OR COALESCE(d.customer_id, -1) <> COALESCE(s.customer_id, -1)
        OR COALESCE(d.order_date, TIMESTAMP '1900-01-01') <> COALESCE(s.order_date, TIMESTAMP '1900-01-01')
        OR COALESCE(d.ship_date, TIMESTAMP '1900-01-01') <> COALESCE(s.ship_date, TIMESTAMP '1900-01-01')
        OR COALESCE(d.sales_order_number, '') <> COALESCE(s.sales_order_number, '')
),
record_changed_global AS (
    SELECT 
        s.sales_order_id,
        s.sales_order_detail_id,
        s.product_id,
        s.customer_id,
        s.order_date,
        s.ship_date,
        s.sales_order_number,
        s.updated_at
    FROM stg_order__source s
    WHERE s.sales_order_detail_id NOT IN (
        SELECT DISTINCT sales_order_detail_id 
        FROM iceberg.gold.dim_order
    )
),
update_records_local AS (
    DELETE FROM iceberg.gold.dim_order d
    WHERE d.sales_order_detail_id IN (SELECT sales_order_detail_id FROM record_changed_local)
),
insert_records AS (
    SELECT * FROM record_changed_local
    UNION ALL
    SELECT * FROM record_changed_global
)
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
                cast(o.sales_order_detail_id as varchar) || ':' ||
                cast(date(o.updated_at) as varchar)
            )
        )
    )) as sales_order_key,
    o.sales_order_id,
    o.sales_order_detail_id,
    o.product_id,
    o.customer_id,
    o.order_date,
    o.ship_date,
    o.sales_order_number,
    month(o.order_date) as order_month,
    year(o.order_date) as order_year,
    {{data_interval_start}} as created_at,
    {{data_interval_start}} as updated_at
FROM insert_records o;