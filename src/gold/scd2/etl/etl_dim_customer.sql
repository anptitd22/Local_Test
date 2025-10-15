-- UPDATE iceberg.gold.dim_customer 
-- SET is_current = false,
--     active_end = current_timestamp
-- WHERE is_current = true
--     AND active_start < timestamp '{{data_interval_end}}'
--     AND customer_id IN (
--     SELECT DISTINCT customer_id
--     FROM iceberg.gold.stg_customer
--         WHERE updated_at >= timestamp '{{data_interval_start}}'
--         and updated_at < timestamp '{{data_interval_end}}'
--     );


-- INSERT INTO iceberg.gold.dim_customer (
--     customer_key,
--     customer_id,
--     account_number,
--     first_name,
--     middle_name,
--     last_name,
--     full_name,
--     is_current,
--     active_start,
--     active_end
-- )
-- SELECT
--     ABS(from_big_endian_64(
--         xxhash64(
--             to_utf8(
--                 cast(customer_id as varchar) || ':' ||
--                 cast(date(updated_at) as varchar)
--             )
--         )
--     )) as customer_key,
--     customer_id,
--     account_number,
--     first_name,
--     middle_name,
--     last_name,
--     concat_ws(' ', first_name, middle_name, last_name) as full_name,
--     TRUE as is_current,
--     updated_at as active_start,
--     TIMESTAMP '9999-12-31' as active_end
-- FROM iceberg.gold.stg_customer
-- WHERE updated_at >= timestamp '{{data_interval_start}}' 
--     and updated_at < timestamp '{{data_interval_end}}';

WITH stg_customer__source AS (
    SELECT 
        customer_id,
        account_number,
        first_name,
        middle_name,
        last_name,
        updated_at
    FROM iceberg.gold.stg_customer
    WHERE updated_at >= timestamp '{{data_interval_start}}' 
        AND updated_at < timestamp '{{data_interval_end}}'
),
record_changed_local AS (
    SELECT 
        s.customer_id
        , s.account_number
        , s.first_name
        , s.middle_name
        , s.last_name
        , s.created_at
        , s.updated_at
    FROM iceberg.gold.dim_customer d
    JOIN stg_customer__source s
        ON s.customer_id = d.customer_id
        AND d.is_current = true
        AND d.active_start < timestamp '{{data_interval_end}}'
    WHERE
        coalesce(d.account_number,'') <> coalesce(s.account_number,'')
        OR coalesce(d.first_name,'') <> coalesce(s.first_name,'')
        OR coalesce(d.middle_name,'') <> coalesce(s.middle_name,'')
        OR coalesce(d.last_name,'') <> coalesce(s.last_name,'')
),
record_changed_global AS (
    SELECT 
        s.customer_id
        , s.account_number
        , s.first_name
        , s.middle_name
        , s.last_name
        , s.created_at
        , s.updated_at
    FROM stg_customer__source s
    WHERE s.customer_id NOT IN (
        SELECT DISTINCT customer_id 
        FROM iceberg.gold.dim_customer
    )
),
update_records_local AS (
UPDATE iceberg.gold.dim_customer d
SET is_current = false,
    active_end = current_timestamp
WHERE d.is_current = true
  AND d.customer_id IN (SELECT customer_id FROM record_changed_local)
),
insert_records AS (
    SELECT * FROM record_changed_local
    UNION ALL
    SELECT * FROM record_changed_global
)
INSERT INTO iceberg.gold.dim_customer (
    customer_key,
    customer_id,
    account_number,
    first_name,
    middle_name,
    last_name,
    full_name,
    is_current,
    active_start,
    active_end
)
SELECT 
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(c.customer_id as varchar) || ':' ||
                cast(date(c.updated_at) as varchar)
            )
        )
    )) as customer_key,
    c.customer_id,
    c.account_number,
    c.first_name,
    c.middle_name,
    c.last_name,
    concat_ws(' ', c.first_name, c.middle_name, c.last_name) as full_name,
    TRUE as is_current,
    c.updated_at as active_start,
    TIMESTAMP '9999-12-31' as active_end
FROM insert_records c;