INSERT INTO iceberg.gold.dim_date
SELECT
    CAST(format_datetime(d, 'yyyyMMd') AS BIGINT) * 1 
        + (CASE WHEN length(format_datetime(d, 'yyyyMMdd')) = 7 THEN 0 ELSE 0 END) AS date_key,
    d AS the_date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    day(d) AS day,
    day_of_week(d) AS day_of_week,
    format_datetime(d, 'EEEE') AS day_name,
    format_datetime(d, 'MMMM') AS month_name,
    (day_of_week(d) IN (6, 7)) AS is_weekend,
    FALSE AS is_holiday,
    week_of_year(d) AS week_of_year
FROM UNNEST(sequence(DATE '2010-01-01', DATE '2025-12-31', INTERVAL '1' DAY)) AS t(d);