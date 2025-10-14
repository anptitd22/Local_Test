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
from pyiceberg.types import NestedField, StringType, TimestampType, LongType

#env 
path_env = '../../.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'admin')
ACCESS_SECRET = os.getenv('MINIO_ROOT_PASSWORD', 'admin12345')

#arguments
parser = argparse.ArgumentParser()
parser.add_argument('--operation', type=str, required=False, default='upload', help='Operation to perform: upload or delete')
parser.add_argument('--namespace', type=str, required=False, default='silver', help='Namespace of the table')
parser.add_argument('--namespace_etl', type=str, required=False, default='gold', help='Namespace of the ETL table')
parser.add_argument('--file', type=str, required=False, default='../../dataset/datamart/Customers.csv', help='CSV file to upload')
parser.add_argument('--table', type=str, required=False, default='customers', help='Table name to upload or delete')
args = parser.parse_args()

if __name__ == "__main__":
    arrow_schema = pa.schema([
        pa.field("CustomerID", pa.int64(),  nullable=False),
        pa.field("AccountNumber", pa.string(), nullable=True),
        pa.field("FirstName", pa.string(), nullable=True),
        pa.field("MiddleName", pa.string(), nullable=True),
        pa.field("LastName", pa.string(), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=False),
    ])
    iceberg_schema = Schema(
        NestedField(field_id=1, name="CustomerID", field_type=LongType(), required=True),
        NestedField(field_id=2, name="AccountNumber", field_type=StringType(), required=False),
        NestedField(field_id=3, name="FirstName", field_type=StringType(), required=False),
        NestedField(field_id=4, name="MiddleName", field_type=StringType(), required=False),
        NestedField(field_id=5, name="LastName", field_type=StringType(), required=False),
        NestedField(field_id=6, name="CreatedAt", field_type=TimestampType(), required=True),
    )
    catalog=load_catalog(
        "hive",
        **{
            "uri": "thrift://metastore:9083",
            "warehouse": "s3a://lakehouse",
            "s3.endpoint": "http://minio:9000",
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
        , namespace_etl=args.namespace_etl
        , table=args.table
        , catalog=catalog
        , arrow_schema=arrow_schema
        , iceberg_schema=iceberg_schema
    )
    iceberg.drop_missing_data("CustomerID")

    if args.operation == 'upload':
        iceberg.upload_to_iceberg()
    elif args.operation == 'delete':
        iceberg.delete_iceberg_table()
    else:
        raise ValueError("operation must be one of 'upload' or 'delete'")