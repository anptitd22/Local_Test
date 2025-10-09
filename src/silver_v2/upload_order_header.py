import sys
import os
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

import argparse
import pyarrow as pa

from dotenv import load_dotenv
from build.ArrowIceberg import ArrowIcebergMinIO
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, DecimalType, TimestampType, LongType, StringType

#env 
path_env = '../../.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
ACCESS_SECRET = os.getenv('MINIO_ROOT_PASSWORD')

#arguments
parser = argparse.ArgumentParser()
parser.add_argument('--operation', type=str, required=False, default='upload', help='Operation to perform: upload or delete')
parser.add_argument('--namespace', type=str, required=False, default='silver', help='Namespace of the table')
parser.add_argument('--file', type=str, required=False, default='../../dataset/datamart/OrderHeader.csv', help='CSV file to upload')
parser.add_argument('--table', type=str, required=False, default='order_headers', help='Table name to upload or delete')
args = parser.parse_args()

if __name__ == "__main__":
    arrow_schema = pa.schema([
        pa.field("SalesOrderID", pa.int64(),  nullable=False),
        pa.field("OrderDate", pa.timestamp("us"), nullable=True),
        pa.field("ShipDate", pa.timestamp("us"), nullable=True),
        pa.field("SalesOrderNumber", pa.string(), nullable=True),
        pa.field("CustomerID", pa.int64(), nullable=False),
        pa.field("SubTotal", pa.decimal128(18, 4), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=False),
    ])
    iceberg_schema = Schema(
        NestedField(1, "SalesOrderID", LongType(), required=True),
        NestedField(2, "OrderDate", TimestampType(), required=False),
        NestedField(3, "ShipDate", TimestampType(), required=False),
        NestedField(4, "SalesOrderNumber", StringType(), required=False),
        NestedField(5, "CustomerID", LongType(), required=True),
        NestedField(6, "SubTotal", DecimalType(18, 4), required=False),
        NestedField(7, "CreatedAt", TimestampType(), required=True),
    )
    catalog=load_catalog(
        "hive",
        **{
            "uri": "thrift://localhost:9083",
            "warehouse": "s3a://lakehouse",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": ACCESS_KEY,
            "s3.secret-access-key": ACCESS_SECRET,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "s3.ssl.enabled": "false",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    )
    iceberg = ArrowIcebergMinIO(
        file_path=args.file
        , file_type='csv'
        , namespace=args.namespace
        , table=args.table
        , catalog=catalog
        , arrow_schema=arrow_schema
        , iceberg_schema=iceberg_schema
    )
    iceberg.drop_missing_data("SalesOrderID")
    iceberg.drop_missing_data("CustomerID")

    if args.operation == 'upload':
        iceberg.upload_to_iceberg()
    elif args.operation == 'delete':
        iceberg.delete_iceberg_table()
    else:
        raise ValueError("operation must be one of 'upload' or 'delete'")