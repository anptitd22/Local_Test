UPDATE iceberg.gold.dim_order 
SET is_current = false,
    active_end = current_timestamp
WHERE is_current = true 
  AND date(active_start) = current_date
  AND NOT EXISTS (
    SELECT 1
    FROM iceberg.gold.stg_order_detail od
    LEFT JOIN iceberg.gold.stg_order_header oh
    ON od.sales_order_id = oh.sales_order_id
    WHERE od.sales_order_detail_id = dim_order.sales_order_detail_id
      AND date(od.updated_at) = current_date
  );

MERGE INTO iceberg.gold.dim_order AS trg
USING (
    SELECT
        od.sales_order_detail_id as sales_order_key
        , od.sales_order_id as sales_order_id
        , od.sales_order_detail_id as sales_order_detail_id
        , od.product_id as product_id
        , oh.customer_id as customer_id
        , oh.order_date as order_date
        , oh.ship_date as ship_date
        , oh.sales_order_number as sales_order_number
        , month(oh.order_date) as order_month
        , year(oh.order_date) as order_year
        , od.updated_at
    FROM iceberg.gold.stg_order_detail od
    LEFT JOIN iceberg.gold.stg_order_header oh
    ON od.sales_order_id = oh.sales_order_id
) AS src
ON trg.sales_order_detail_id = src.sales_order_detail_id
AND trg.is_current = TRUE 
AND date(trg.active_start) = date(src.updated_at)

-- Update 
WHEN MATCHED AND (
       trg.sales_order_id <> src.sales_order_id
    OR trg.product_id <> src.product_id
    OR trg.customer_id <> src.customer_id
    OR trg.order_date <> src.order_date
    OR trg.ship_date <> src.ship_date
    OR trg.sales_order_number <> src.sales_order_number
    OR trg.order_month <> src.order_month
    OR trg.order_year <> src.order_year
) THEN
    UPDATE SET
        is_current = FALSE,
        active_end = src.updated_at

-- Insert 
WHEN NOT MATCHED THEN
    INSERT (
        sales_order_key
        , sales_order_id
        , sales_order_detail_id
        , product_id
        , customer_id
        , order_date
        , ship_date
        , sales_order_number
        , order_month
        , order_year
        , is_current
        , active_start
        , active_end
    )
    VALUES (
        ABS(from_big_endian_64(
            xxhash64(
                to_utf8(
                    cast(src.sales_order_detail_id as varchar) || ':' ||
                    cast(src.updated_at as varchar)
                )
            )
        ))
        , src.sales_order_id
        , src.sales_order_detail_id
        , src.product_id
        , src.customer_id
        , src.order_date
        , src.ship_date
        , src.sales_order_number
        , src.order_month
        , src.order_year
        , TRUE
        , src.updated_at
        , TIMESTAMP '9999-12-31'
    );