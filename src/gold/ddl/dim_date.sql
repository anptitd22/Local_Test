create table if not exists iceberg.silver.dim_date (
    date_key bigint      
    ,the_date DATE 
    , year INTEGER
    , quarter INTEGER
    , month INTEGER
    , day INTEGER
    , day_of_week INTEGER
    , day_name varchar
    , month_name varchar
    , is_weekend BOOLEAN
    , is_holiday BOOLEAN
    , week_of_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(the_date)']
);