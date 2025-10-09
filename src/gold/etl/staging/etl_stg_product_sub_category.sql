INSERT INTO iceberg.silver.stg_product_sub_category
(
    product_sub_category_id 
    , product_category_id  
    , name 
    , created_at 
    , updated_at 
)
SELECT
    CAST(productsubcategoryid AS BIGINT) AS product_sub_category_id
    , CAST(productcategoryid AS BIGINT) AS product_category_id
    , CAST(name AS VARCHAR) AS name
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.product_sub_categories;