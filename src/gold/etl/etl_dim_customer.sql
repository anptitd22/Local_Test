UPDATE iceberg.gold.dim_customer 
SET is_current = false,
    active_end   = current_timestamp
WHERE is_current = true and date(active_start) = date(current_date) 
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.gold.stg_customer s
    WHERE s.customer_id = customer_id and date(s.updated_at) = date(current_date)
  );

MERGE INTO iceberg.gold.dim_customer AS trg
USING (
    SELECT
        customer_id
        , account_number
        , first_name
        , middle_name
        , last_name
        , concat_ws(' ', first_name, middle_name, last_name) AS full_name
        , updated_at
    FROM iceberg.gold.stg_customer
) AS src
ON trg.customer_id = src.customer_id
AND trg.is_current = TRUE AND date(trg.active_start) = date(src.updated_at)

-- update
WHEN MATCHED AND (
       trg.account_number <> src.account_number
    OR trg.first_name <> src.first_name
    OR trg.middle_name <> src.middle_name
    OR trg.last_name <> src.last_name
    OR trg.full_name <> src.full_name
) THEN
    UPDATE SET
        is_current = FALSE,
        active_end = src.updated_at
-- insert
WHEN NOT MATCHED THEN
    INSERT (
        customer_key
        , customer_id
        , account_number
        , first_name
        , middle_name
        , last_name
        , full_name
        , is_current
        , active_start
        , active_end
    )
    VALUES (
        ABS(from_big_endian_64(
            xxhash64(
                to_utf8(
                    cast(src.customer_id as varchar) || ':' ||
                    cast(current_timestamp as varchar)
                )
            )
        ))
        , src.customer_id
        , src.account_number
        , src.first_name
        , src.middle_name
        , src.last_name
        , src.full_name
        , TRUE
        , src.updated_at
        , TIMESTAMP '9999-12-31'
    );