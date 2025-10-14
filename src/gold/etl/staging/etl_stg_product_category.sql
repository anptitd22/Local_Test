DELETE FROM iceberg.gold.stg_product_category
WHERE date(updated_at) = current_date;

INSERT INTO iceberg.gold.stg_product_category
(
    product_category_id  
    , name 
    , created_at 
    , updated_at 
)
SELECT
    CAST(productcategoryid AS BIGINT) AS product_category_id
    , CAST(name AS VARCHAR) AS name
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.product_categories;
-- WHERE date(createdat) = current_date;