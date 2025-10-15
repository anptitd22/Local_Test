UPDATE iceberg.gold.dim_product 
SET is_current = false,
    active_end = current_timestamp
WHERE is_current = true 
  AND product_id IN (
    SELECT DISTINCT product_id 
    FROM iceberg.gold.stg_product 
    WHERE updated_at >= timestamp '{{data_interval_start}}'
        and updated_at < timestamp '{{data_interval_end}}'
  );

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
    category_name,
    is_current,
    active_start,
    active_end
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
    pc.product_category_id,
    psc.product_sub_category_id,
    p.list_price as product_list_price,
    p.name as product_name,
    p.color as product_color,
    p.size as product_size,
    psc.name as sub_category_name,
    pc.name as category_name,
    TRUE as is_current,
    p.updated_at as active_start,
    TIMESTAMP '9999-12-31' as active_end
FROM iceberg.gold.stg_product p
LEFT JOIN iceberg.gold.stg_product_sub_category psc
    ON p.product_sub_category_id = psc.product_sub_category_id 
    AND psc.updated_at >= timestamp '{{data_interval_start}}'
    and psc.updated_at < timestamp '{{data_interval_end}}'
LEFT JOIN iceberg.gold.stg_product_category pc
    ON psc.product_category_id = pc.product_category_id 
    AND pc.updated_at >= timestamp '{{data_interval_start}}'
    and pc.updated_at < timestamp '{{data_interval_end}}'
WHERE p.updated_at >= timestamp '{{data_interval_start}}' 
    and p.updated_at < timestamp '{{data_interval_end}}';