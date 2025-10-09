INSERT INTO iceberg.silver.stg_product
(
    product_id bigint
    , name varchar
    , color varchar
    , list_price decimal(18,4)
    , size varchar
    , product_sub_category_id bigint
)
WITH product__source AS (
    SELECT * FROM iceberg.silver.products
)
, product__rename_column AS (
    SELECT
        productid as product_id
        , name as name
        , color as color
        , listprice as list_price
        , size as size
        , productsubcategoryid as product_sub_category_id
    FROM product__source
)
, product__cast_type AS (
    SELECT
        CAST(product_id AS BIGINT) AS product_id
        , CAST(name AS VARCHAR) AS name
        , CAST(color AS VARCHAR) AS color
        , CAST(list_price AS DECIMAL(18,4)) AS list_price
        , CAST(size AS VARCHAR) AS size
        , CAST(product_sub_category_id AS BIGINT) AS product_sub_category_id
    FROM product__rename_column
)
, product__add_fault_record AS (
    SELECT
        product_id
        , name
        , color
        , list_price
        , size
        , product_sub_category_id 
    FROM product__cast_type
    UNION ALL
    SELECT
        0 AS product_id
        , 'undefined' AS name
        , 'undefined' AS color
        , 0.0000 AS list_price
        , 'undefined' AS size
        , 0 AS product_sub_category_id
    UNION ALL
    SELECT
        -1 AS product_id
        , 'unknown' AS name
        , 'unknown' AS color
        , 0.0000 AS list_price
        , 'unknown' AS size
        , -1 AS product_sub_category_id
)
SELECT
    product_id
    , COALESCE(name, 'unknown') AS name 
    , COALESCE(color, 'unknown') AS color
    , COALESCE(list_price, 0.0000) AS list_price
    , COALESCE(size, 'unknown') AS size
    , COALESCE(product_sub_category_id, -1) AS product_sub_category_id
FROM product__add_fault_record;
