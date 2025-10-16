TRUNCATE TABLE iceberg.gold.dim_product;

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
    created_at,
    updated_at
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
    CAST('{{data_interval_start}}' AS TIMESTAMP) as created_at,
    CAST('{{data_interval_start}}' AS TIMESTAMP) as updated_at
FROM iceberg.gold.stg_product p
LEFT JOIN iceberg.gold.stg_product_sub_category psc
    ON p.product_sub_category_id = psc.product_sub_category_id 
LEFT JOIN iceberg.gold.stg_product_category pc
    ON psc.product_category_id = pc.product_category_id;