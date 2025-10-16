DELETE FROM iceberg.gold.stg_product_category
WHERE updated_at >= timestamp '{{data_interval_start}}' and updated_at < timestamp '{{data_interval_end}}';

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
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS created_at
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS updated_at
FROM iceberg.silver.product_categories;
-- WHERE date(createdat) = current_date;