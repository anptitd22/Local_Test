INSERT INTO iceberg.silver.dim_product
(
    product_key
    , product_id
    , product_category_id
    , product_sub_category_id
    , product_name
    , product_color
    , product_size
    , sub_category_name
    , category_name
    , created_at
    , updated_at
)
SELECT
    p.productid as product_key
    , p.productid as product_id
    , pc.productcategoryid as product_category_id
    , psc.productsubcategoryid as product_sub_category_id
    , p.name as product_name
    , p.color as product_color
    , p.size as product_size
    , psc.name as sub_category_name
    , pc.name as category_name
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.product p
JOIN iceberg.silver.product_sub_category psc
ON p.productsubcategoryid = psc.productsubcategoryid
JOIN iceberg.silver.product_category pc
ON psc.productcategoryid = pc.productcategoryid;