INSERT INTO iceberg.silver.stg_product_sub_category
(
    product_sub_category_id bigint
    , product_category_id bigint 
    , name varchar
)
WITH product_sub_category__source AS (
    SELECT * FROM iceberg.silver.product_sub_categories
)
, product_sub_category__rename_column AS (
    SELECT
        productsubcategoryid as product_sub_category_id
        , productcategoryid as product_category_id 
        , name as name
    FROM product_sub_category__source    
)
, product_sub_category__cast_type AS (
    SELECT
        CAST(product_sub_category_id AS BIGINT) AS product_sub_category_id
        , CAST(product_category_id AS BIGINT) AS product_category_id
        , CAST(name AS VARCHAR) AS name
    FROM product_sub_category__rename_column
)
, product_sub_category__add_fault_record AS (
    SELECT
        product_sub_category_id
        , product_category_id
        , name
    FROM product_sub_category__cast_type
    UNION ALL
    SELECT
        0 AS product_sub_category_id
        , 0 AS product_category_id
        , 'undefined' AS name
    UNION ALL
    SELECT
        -1 AS product_sub_category_id
        , -1 AS product_category_id
        , 'unknown' AS name
)
SELECT
    product_sub_category_id
    , COALESCE(product_category_id, -1) AS product_category_id
    , COALESCE(name, 'unknown') AS name
FROM product_sub_category__add_fault_record;