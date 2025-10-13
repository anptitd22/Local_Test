TRUNCATE TABLE iceberg.gold.stg_product;

INSERT INTO iceberg.gold.stg_product
(
    product_id 
    , name 
    , color 
    , list_price 
    , size 
    , product_sub_category_id 
    , created_at 
    , updated_at 
)
SELECT
    CAST(productid AS BIGINT) AS product_id
    , CAST(name AS VARCHAR) AS name
    , CAST(color AS VARCHAR) AS color
    , CAST(listprice AS DECIMAL(18,4)) AS list_price
    , CAST(size AS VARCHAR) AS size
    , CAST(productsubcategoryid AS BIGINT) AS product_sub_category_id
    , current_timestamp AS created_at
    , current_timestamp AS updated_at
FROM iceberg.silver.products;
