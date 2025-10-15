DELETE FROM iceberg.gold.stg_product
WHERE updated_at >= timestamp '{{data_interval_start}}' and updated_at < timestamp '{{data_interval_end}}';

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
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS created_at
    , CAST('{{data_interval_start}}' AS TIMESTAMP) AS updated_at
FROM iceberg.silver.products;
-- WHERE date(createdat) = current_date;
