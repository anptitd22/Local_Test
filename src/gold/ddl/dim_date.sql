create table if not exists <CATALOG>.<SCHEMA>.fact_sales (      
    the_date DATE not null primary key,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,    
    day_name VARCHAR,
    month_name VARCHAR,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,      
    week_of_year INTEGER,
    fiscal_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['days(the_date)']
);