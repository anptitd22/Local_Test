import argparse
import datetime
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType, LongType, DecimalType
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
args = parser.parse_args()

#variable
file_path = '../../dataset/datamart/OrderDetail.csv'
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
        pa.field("SalesOrderID", pa.int64(),  nullable=False),
        pa.field("SalesOrderDetailID", pa.int64(), nullable=False),
        pa.field("ProductID", pa.int64(), nullable=False),
        pa.field("OrderQty", pa.int64(), nullable=True),
        pa.field("UnitPrice", pa.decimal128(18, 4), nullable=True),
        pa.field("UnitPriceDiscount", pa.decimal128(18, 4), nullable=True),
        pa.field("CreatedAt", pa.timestamp("us"), nullable=False),
    ])
    df = drop_missing_data(df, "SalesOrderID")
    df = drop_missing_data(df, "SalesOrderDetailID")
    df = drop_missing_data(df, "ProductID")

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
        NestedField(1, "SalesOrderID", LongType(), required=True),
        NestedField(2, "SalesOrderDetailID", LongType(), required=True),
        NestedField(3, "ProductID", LongType(), required=True),
        NestedField(4, "OrderQty", LongType(), required=False),
        NestedField(5, "UnitPrice", DecimalType(18, 4), required=False),
        NestedField(6, "UnitPriceDiscount", DecimalType(18, 4), required=False),
        NestedField(7, "CreatedAt", TimestampType(), required=True),
    )
    try:
        created_id = schema.find_field("CreatedAt").field_id
        partition_silver = PartitionSpec(
            PartitionField(field_id=1001, source_id=created_id, transform=DayTransform(), name="created_at_day")
        )
        tbl = catalog.create_table("silver.order_detail", schema=schema, partition_spec=partition_silver)
    except:
        tbl = catalog.load_table("silver.order_detail")
    
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
        catalog.drop_table("silver.order_detail")
        print("Table 'silver.order_detail' has been deleted.")
    except Exception as e:
        print(f"Error deleting table: {e}")

if __name__ == "__main__":
    operation = args.operation
    if operation == 'upload':
        df = read_csv(file_path)
        upload_to_iceberg(df)
    elif operation == 'delete':
        delete_iceberg_table()



