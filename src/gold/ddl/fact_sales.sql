create table if not exists <CATALOG>.<SCHEMA>.fact_sales (
    sales_key bigint not null primary key generated always as identity (start with 1 increment by 1)
    , product_key bigint not null
    , customer_key bigint not null
    , sales_order_key bigint not null
    , sub_total decimal(18,4)
    , unit_price decimal(18,4)
    , unit_price_discount decimal(18,4)
    , order_qty bigint
    , foreign key (product_key) references <CATALOG>.<SCHEMA>.dim_product(product_key)
    , foreign key (customer_key) references <CATALOG>.<SCHEMA>.dim_customer(customer_key)
    , foreign key (sales_order_key) references <CATALOG>.<SCHEMA>.dim_order(sales_order_key)
    , created_at timestamp
    , updated_at timestamp
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['days(updated_at)']
);