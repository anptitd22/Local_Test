TRUNCATE TABLE iceberg.gold.dim_customer;

INSERT INTO iceberg.gold.dim_customer
(
    customer_key
    , customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , full_name
    , is_current BOOLEAN 
    , active_start timestamp 
    , active_end timestamp 
    )
SELECT
    ABS(from_big_endian_64(
            xxhash64(
                to_utf8(
                    cast(customer_id as varchar) || ':' ||
                    cast(date(updated_at) as varchar)
                )
            )
        )) as customer_key
    , customer_id as customer_id
    , account_number
    , first_name
    , middle_name
    , last_name
    , concat_ws(' ', first_name, middle_name, last_name) as full_name
    , TRUE as is_current
    , updated_at as active_start
    , TIMESTAMP '9999-12-31' as active_end
FROM iceberg.gold.stg_customer;