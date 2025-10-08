import argparse
import datetime
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType, LongType, StringType, DecimalType
from pyiceberg.partitioning import PartitionSpec, PartitionField, DayTransform
# import boto3

from dotenv import load_dotenv
import os

from pyarrow import Table as DataFrame

#env 
path_env = '../../.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
ACCESS_SECRET = os.getenv('MINIO_ROOT_PASSWORD')

#arguments
parser = argparse.ArgumentParser()
parser.add_argument('--operation', type=str, required=False, default='upload', help='Operation to perform: upload or delete')
parser.add_argument('--namespace', type=str, required=False, default='silver', help='Namespace of the table')
parser.add_argument('--file', type=str, required=False, default='../../dataset/datamart/ProductCategory.csv', help='CSV file to upload')
args = parser.parse_args()

#variable
create_at = datetime.datetime.now().isoformat(timespec="seconds")

#main functions
def drop_missing_data(table: DataFrame, field: str) -> DataFrame:
    mask = pc.invert(pc.is_null(table[field]))
    return table.filter(mask)

def read_csv(file_path: str) -> DataFrame:
    df = csv.read_csv(file_path)
    df = df.append_column("CreatedAt", pc.strptime(pa.array([create_at] * len(df)), format="%Y-%m-%dT%H:%M:%S", unit="us"))
    return df

def upload_to_iceberg(df: DataFrame) -> None:
    arrow_schema = pa.schema([
        pa.field("ProductCategoryID", pa.int64(),  nullable=False),
        pa.field("Name", pa.string(), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=False),
    ])
    df = drop_missing_data(df, "ProductCategoryID")

    df = df.cast(arrow_schema)
    catalog = load_catalog(
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
    schema = Schema(
        NestedField(1, "ProductCategoryID", LongType(), required=True),
        NestedField(2, "Name", StringType(), required=False),
        NestedField(3, "CreatedAt", TimestampType(), required=True),
    )
    try:
        created_id = schema.find_field("CreatedAt").field_id
        partition_silver = PartitionSpec(
            PartitionField(field_id=1001, source_id=created_id, transform=DayTransform(), name="created_at_day")
        )
        tbl = catalog.create_table(f"{args.namespace}.product_category", schema=schema, partition_spec=partition_silver)
    except:
        tbl = catalog.load_table(f"{args.namespace}.product_category")

    tbl.append(df)

    result = tbl.scan().to_arrow()
    print(result)

def delete_iceberg_table() -> None:
    catalog = load_catalog(
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
    try:
        catalog.drop_table(f"{args.namespace}.product_category")
        print(f"Table '{args.namespace}.product_category' has been deleted.")
    except Exception as e:
        print(f"Error deleting table: {e}")
        raise

if __name__ == "__main__":
    operation = args.operation
    if operation == 'upload':
        df = read_csv(args.file)
        upload_to_iceberg(df)
    elif operation == 'delete':
        delete_iceberg_table()
