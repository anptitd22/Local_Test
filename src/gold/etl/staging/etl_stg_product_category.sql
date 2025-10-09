INSERT INTO iceberg.silver.stg_product_category
(
    product_category_id bigint 
    , name varchar
)
WITH product_category__source AS (
    SELECT * FROM iceberg.silver.product_categories
)
, product_category__rename_column AS (
    SELECT
        productcategoryid as product_category_id
        , name as name
    FROM product_category__source    
)
, product_category__cast_type AS (
    SELECT
        CAST(product_category_id AS BIGINT) AS product_category_id
        , CAST(name AS VARCHAR) AS name
    FROM product_category__rename_column
)
, product_category__add_fault_record AS (
    SELECT
        product_category_id
        , name
    FROM product_category__cast_type
    UNION ALL
    SELECT
        0 AS product_category_id
        , 'undefined' AS name
    UNION ALL
    SELECT
        -1 AS product_category_id
        , 'unknown' AS name
)
SELECT
    product_category_id
    , COALESCE(name, 'unknown') AS name
FROM product_category__add_fault_record;