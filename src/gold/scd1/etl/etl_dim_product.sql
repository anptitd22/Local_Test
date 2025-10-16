-- UPDATE iceberg.gold.dim_product 
-- SET is_current = false,
--     active_end = current_timestamp
-- WHERE is_current = true 
--     and active_start < timestamp '{{data_interval_end}}'
--     AND product_id IN (
--         SELECT DISTINCT product_id 
--         FROM iceberg.gold.stg_product 
--         WHERE updated_at >= timestamp '{{data_interval_start}}'
--             and updated_at < timestamp '{{data_interval_end}}'
--     );

-- INSERT INTO iceberg.gold.dim_product (
--     product_key,
--     product_id,
--     product_category_id,
--     product_sub_category_id,
--     product_list_price,
--     product_name,
--     product_color,
--     product_size,
--     sub_category_name,
--     category_name,
--     is_current,
--     active_start,
--     active_end
-- )
-- SELECT
--     ABS(from_big_endian_64(
--         xxhash64(
--             to_utf8(
--                 cast(p.product_id as varchar) || ':' ||
--                 cast(date(p.updated_at) as varchar)
--             )
--         )
--     )) as product_key,
--     p.product_id,
--     pc.product_category_id,
--     psc.product_sub_category_id,
--     p.list_price as product_list_price,
--     p.name as product_name,
--     p.color as product_color,
--     p.size as product_size,
--     psc.name as sub_category_name,
--     pc.name as category_name,
--     TRUE as is_current,
--     p.updated_at as active_start,
--     TIMESTAMP '9999-12-31' as active_end
-- FROM iceberg.gold.stg_product p
-- LEFT JOIN iceberg.gold.stg_product_sub_category psc
--     ON p.product_sub_category_id = psc.product_sub_category_id 
--     AND psc.updated_at >= timestamp '{{data_interval_start}}'
--     and psc.updated_at < timestamp '{{data_interval_end}}'
-- LEFT JOIN iceberg.gold.stg_product_category pc
--     ON psc.product_category_id = pc.product_category_id 
--     AND pc.updated_at >= timestamp '{{data_interval_start}}'
--     and pc.updated_at < timestamp '{{data_interval_end}}'
-- WHERE p.updated_at >= timestamp '{{data_interval_start}}' 
--     and p.updated_at < timestamp '{{data_interval_end}}';

WITH stg_product__source AS (
    SELECT 
        p.product_id,
        p.product_sub_category_id,
        p.list_price,
        p.name,
        p.color,
        p.size,
        p.updated_at,
        psc.product_category_id,
        psc.name as sub_category_name,
        pc.name as category_name,
        p.updated_at
    FROM iceberg.gold.stg_product p
    LEFT JOIN iceberg.gold.stg_product_sub_category psc
        ON p.product_sub_category_id = psc.product_sub_category_id 
        AND psc.updated_at >= timestamp '{{data_interval_start}}'
        AND psc.updated_at < timestamp '{{data_interval_end}}'
    LEFT JOIN iceberg.gold.stg_product_category pc
        ON psc.product_category_id = pc.product_category_id 
        AND pc.updated_at >= timestamp '{{data_interval_start}}'
        AND pc.updated_at < timestamp '{{data_interval_end}}'
    WHERE p.updated_at >= timestamp '{{data_interval_start}}' 
        AND p.updated_at < timestamp '{{data_interval_end}}'
),
record_changed_local AS (
    SELECT 
        s.product_id,
        s.product_sub_category_id,
        s.product_category_id,
        s.list_price,
        s.name,
        s.color,
        s.size,
        s.sub_category_name,
        s.category_name,
        s.updated_at
    FROM iceberg.gold.dim_product d
    JOIN stg_product__source s
        ON s.product_id = d.product_id
        AND d.updated_at < timestamp '{{data_interval_end}}'
    WHERE
        COALESCE(d.product_category_id, -1) <> COALESCE(s.product_category_id, -1)
        OR COALESCE(d.product_sub_category_id, -1) <> COALESCE(s.product_sub_category_id, -1)
        OR COALESCE(d.product_list_price, -1) <> COALESCE(s.list_price, -1)
        OR COALESCE(d.product_name, '') <> COALESCE(s.name, '')
        OR COALESCE(d.product_color, '') <> COALESCE(s.color, '')
        OR COALESCE(d.product_size, '') <> COALESCE(s.size, '')
        OR COALESCE(d.sub_category_name, '') <> COALESCE(s.sub_category_name, '')
        OR COALESCE(d.category_name, '') <> COALESCE(s.category_name, '')
),
record_changed_global AS (
    SELECT 
        s.product_id,
        s.product_sub_category_id,
        s.product_category_id,
        s.list_price,
        s.name,
        s.color,
        s.size,
        s.sub_category_name,
        s.category_name,
        s.updated_at
    FROM stg_product__source s
    WHERE s.product_id NOT IN (
        SELECT DISTINCT product_id 
        FROM iceberg.gold.dim_product
    )
),
update_records_local AS (
    DELETE FROM iceberg.gold.dim_product d
    WHERE d.product_id IN (SELECT product_id FROM record_changed_local)
),
insert_records AS (
    SELECT * FROM record_changed_local
    UNION ALL
    SELECT * FROM record_changed_global
)
INSERT INTO iceberg.gold.dim_product (
    product_key,
    product_id,
    product_category_id,
    product_sub_category_id,
    product_list_price,
    product_name,
    product_color,
    product_size,
    sub_category_name,
    category_name
)
SELECT
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(p.product_id as varchar) || ':' ||
                cast(date(p.updated_at) as varchar)
            )
        )
    )) as product_key,
    p.product_id,
    p.product_category_id,
    p.product_sub_category_id,
    p.list_price as product_list_price,
    p.name as product_name,
    p.color as product_color,
    p.size as product_size,
    p.sub_category_name,
    p.category_name,
    {{data_interval_start}} as created_at,
    {{data_interval_start}} as updated_at
FROM insert_records p;