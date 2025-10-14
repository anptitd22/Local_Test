TRUNCATE TABLE iceberg.gold.fact_sales_month;

INSERT INTO iceberg.gold.fact_sales_month
(
    sales_month_key
    , customer_key
    , month_sub_total
    , month
    , year
)
SELECT
    ABS(from_big_endian_64(
        xxhash64(
            to_utf8(
                cast(fs.customer_key  as varchar) || ':' ||
                cast(do.order_month as varchar) || ':' ||
                cast(do.order_year as varchar)
            )
        ) 
    )) AS sales_month_key
    , fs.customer_key as customer_key
    , SUM(fs.order_unit_sub_total) as month_sub_total
    , do.order_month as month
    , do.order_year as year
FROM iceberg.gold.fact_sales fs
LEFT JOIN iceberg.gold.dim_order do
ON fs.sales_order_key = do.sales_order_key
GROUP BY
    fs.customer_key
    , do.order_month
    , do.order_year
;