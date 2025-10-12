UPDATE iceberg.gold.dim_product 
SET is_current = false,
    active_end = current_timestamp
WHERE is_current = true 
  AND date(active_start) = date(current_date)
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.gold.stg_product s
    WHERE s.product_id = dim_product.product_id
      AND date(s.updated_at) = date(current_date)
  );

MERGE INTO iceberg.gold.dim_product AS trg
USING (
    SELECT
        p.product_id
        , pc.product_category_id
        , psc.product_sub_category_id
        , p.list_price as product_list_price
        , p.name as product_name
        , p.color as product_color
        , p.size as product_size
        , psc.name as sub_category_name
        , pc.name as category_name
        , p.updated_at
    FROM iceberg.gold.stg_product p
    LEFT JOIN iceberg.gold.stg_product_sub_category psc
    ON p.product_sub_category_id = psc.product_sub_category_id
    LEFT JOIN iceberg.gold.stg_product_category pc
    ON psc.product_category_id = pc.product_category_id
) AS src
ON trg.product_id = src.product_id
AND trg.is_current = TRUE 
AND date(trg.active_start) = date(src.updated_at)

-- Update 
WHEN MATCHED AND (
       trg.product_category_id <> src.product_category_id
    OR trg.product_sub_category_id <> src.product_sub_category_id
    OR trg.product_list_price <> src.product_list_price
    OR trg.product_name <> src.product_name
    OR trg.product_color <> src.product_color
    OR trg.product_size <> src.product_size
    OR trg.sub_category_name <> src.sub_category_name
    OR trg.category_name <> src.category_name
) THEN
    UPDATE SET
        is_current = FALSE,
        active_end = src.updated_at
-- Insert 
WHEN NOT MATCHED THEN
    INSERT (
        product_key
        , product_id
        , product_category_id
        , product_sub_category_id
        , product_list_price
        , product_name
        , product_color
        , product_size
        , sub_category_name
        , category_name
        , is_current
        , active_start
        , active_end
    )
    VALUES (
        ABS(from_big_endian_64(
            xxhash64(
                to_utf8(
                    cast(src.product_id as varchar) || ':' ||
                    cast(current_timestamp as varchar)
                )
            )
        ))
        , src.product_id
        , src.product_category_id
        , src.product_sub_category_id
        , src.product_list_price
        , src.product_name
        , src.product_color
        , src.product_size
        , src.sub_category_name
        , src.category_name
        , TRUE
        , src.updated_at
        , TIMESTAMP '9999-12-31'
    );