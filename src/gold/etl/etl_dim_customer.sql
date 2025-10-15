UPDATE iceberg.gold.dim_customer 
SET is_current = false,
    active_end = current_timestamp
WHERE is_current = true 
  AND customer_id IN (
    SELECT DISTINCT customer_id 
    FROM iceberg.gold.stg_customer 
    WHERE updated_at >= timestamp '{{data_interval_start}}' 
    and updated_at < timestamp '{{data_interval_end}}'
  );


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
                cast(customer_id as varchar) || ':' ||
                cast(date(updated_at) as varchar)
            )
        )
    )) as customer_key,
    customer_id,
    account_number,
    first_name,
    middle_name,
    last_name,
    concat_ws(' ', first_name, middle_name, last_name) as full_name,
    TRUE as is_current,
    updated_at as active_start,
    TIMESTAMP '9999-12-31' as active_end
FROM iceberg.gold.stg_customer
WHERE updated_at >= timestamp '{{data_interval_start}}' 
    and updated_at < timestamp '{{data_interval_end}}';