from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType, LongType, DecimalType
import pyarrow as pa

SCHEMA_ICEBERG = {
    "products": Schema(
        NestedField(1, "ProductID", LongType(), required=False),
        NestedField(2, "Name", StringType(), required=False),
        NestedField(3, "Color", StringType(), required=False),
        NestedField(4, "ListPrice", DecimalType(18,4), required=False),
        NestedField(5, "Size", StringType(), required=False),
        NestedField(6, "ProductSubcategoryID", LongType(), required=False),
        NestedField(7, "CreatedAt", TimestampType(), required=False),
    ),
    "product_sub_categories": Schema(
        NestedField(1, "ProductSubcategoryID", LongType(), required=False),
        NestedField(2, "ProductCategoryID", LongType(), required=False),
        NestedField(3, "Name", StringType(), required=False),
        NestedField(4, "CreatedAt", TimestampType(), required=False),
    ),
    "product_categories": Schema(
        NestedField(1, "ProductCategoryID", LongType(), required=False),
        NestedField(2, "Name", StringType(), required=False),
        NestedField(3, "CreatedAt", TimestampType(), required=False),
    ),
    "order_headers": Schema(
        NestedField(1, "SalesOrderID", LongType(), required=False),
        NestedField(2, "OrderDate", TimestampType(), required=False),
        NestedField(3, "ShipDate", TimestampType(), required=False),
        NestedField(4, "SalesOrderNumber", StringType(), required=False),
        NestedField(5, "CustomerID", LongType(), required=False),
        NestedField(6, "SubTotal", DecimalType(18,4), required=False),
        NestedField(7, "CreatedAt", TimestampType(), required=False),
    ),
    "order_details": Schema(
        NestedField(1, "SalesOrderID", LongType(), required=False),
        NestedField(2, "SalesOrderDetailID", LongType(), required=False),
        NestedField(3, "ProductID", LongType(), required=False),
        NestedField(4, "OrderQty", LongType(), required=False),
        NestedField(5, "UnitPrice", DecimalType(18,4), required=False),
        NestedField(6, "UnitPriceDiscount", DecimalType(18,4), required=False),
        NestedField(7, "CreatedAt", TimestampType(), required=False),
    ),
    "customers": Schema(
        NestedField(1, "CustomerID", LongType(), required=False),
        NestedField(2, "AccountNumber", StringType(), required=False),
        NestedField(3, "FirstName", StringType(), required=False),
        NestedField(4, "MiddleName", StringType(), required=False),
        NestedField(5, "LastName", StringType(), required=False),
        NestedField(6, "CreatedAt", TimestampType(), required=False),
    ),
}

SCHEMA_ARROW = {
    "products": pa.schema([
        pa.field("ProductID", pa.int64(),  nullable=True),
        pa.field("Name", pa.string(), nullable=True),
        pa.field("Color", pa.string(), nullable=True),
        pa.field("ListPrice", pa.decimal128(18, 4), nullable=True),
        pa.field("Size", pa.string(), nullable=True),
        pa.field("ProductSubcategoryID", pa.int64(), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=True),
    ]),
    "product_categories": pa.schema([
        pa.field("ProductCategoryID", pa.int64(),  nullable=True),
        pa.field("Name", pa.string(), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=True),
    ]),
    "product_sub_categories": pa.schema([
        pa.field("ProductSubcategoryID", pa.int64(),  nullable=True),
        pa.field("ProductCategoryID", pa.int64(), nullable=True),
        pa.field("Name", pa.string(), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=True),
    ]),
    "order_headers": pa.schema([
        pa.field("SalesOrderID", pa.int64(),  nullable=True),
        pa.field("OrderDate", pa.timestamp("us"), nullable=True),
        pa.field("ShipDate", pa.timestamp("us"), nullable=True),
        pa.field("SalesOrderNumber", pa.string(), nullable=True),
        pa.field("CustomerID", pa.int64(), nullable=True),
        pa.field("SubTotal", pa.decimal128(18, 4), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=True),
    ]),
    "order_details": pa.schema([
        pa.field("SalesOrderID", pa.int64(),  nullable=True),
        pa.field("SalesOrderDetailID", pa.int64(), nullable=True),
        pa.field("ProductID", pa.int64(), nullable=True),
        pa.field("OrderQty", pa.int64(), nullable=True),
        pa.field("UnitPrice", pa.decimal128(18, 4), nullable=True),
        pa.field("UnitPriceDiscount", pa.decimal128(18, 4), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=True),
    ]),
    "customers": pa.schema([
        pa.field("CustomerID", pa.int64(),  nullable=True),
        pa.field("AccountNumber", pa.string(), nullable=True),
        pa.field("FirstName", pa.string(), nullable=True),
        pa.field("MiddleName", pa.string(), nullable=True),
        pa.field("LastName", pa.string(), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=True),
    ])
}