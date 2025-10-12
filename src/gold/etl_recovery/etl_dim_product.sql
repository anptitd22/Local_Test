INSERT INTO iceberg.gold.dim_product
(
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
    , created_at
    , updated_at
)
SELECT
    p.product_id as product_key
    , p.product_id as product_id
    , pc.product_category_id as product_category_id
    , psc.product_sub_category_id as product_sub_category_id
    , p.name as product_name
    , p.color as product_color
    , p.size as product_size
    , psc.name as sub_category_name
    , pc.name as category_name
    , p.list_price as product_list_price
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.gold.stg_product p
LEFT JOIN iceberg.gold.stg_product_sub_category psc
ON p.product_sub_category_id = psc.product_sub_category_id
LEFT JOIN iceberg.gold.stg_product_category pc
ON psc.product_category_id = pc.product_category_id;