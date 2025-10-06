import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType, LongType
import boto3

from dotenv import load_dotenv
import os

from pyarrow import Table as DataFrame

path_env = './.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
ACCESS_SECRET = os.getenv('MINIO_ROOT_PASSWORD')

file_path = './dataset/healthcare/doctors.csv'


def dropMissingData(table: DataFrame, field: str) -> DataFrame:
    mask = pc.not_(pc.is_null(table[field]))
    filtered_table = table.filter(mask)
    return filtered_table

def create_bucket_if_not_exists(bucket_name="lakehouse"):
    s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=ACCESS_SECRET,
            region_name="us-east-1",
        )

    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        s3.create_bucket(Bucket=bucket_name)

def read_csv(file_path: str) -> DataFrame:
    df = csv.read_csv(file_path)
    df = df.append_column("created_at", pc.strptime(pa.array(["2024-01-01"] * len(df)), format="%Y-%m-%d", unit="s"))
    return df

def upload_to_iceberg(df: DataFrame) -> None:
    arrow_schema = pa.schema([
        pa.field("doctor_id", pa.int64(),  nullable=False),
        pa.field("Name", pa.string(), nullable=False),
        pa.field("last_name", pa.string(), nullable=False),
        pa.field("specialization", pa.string(), nullable=False),
        pa.field("phone", pa.string(), nullable=False),
        pa.field("email", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp("us"), nullable=False),
    ])
    df = dropMissingData(df, "doctor_id")

    df = df.cast(arrow_schema)
    catalog = load_catalog(
        "hive",
        **{
            "uri": "thrift://localhost:9083",
            "warehouse": "s3a://lakehouse/",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": ACCESS_KEY,
            "s3.secret-access-key": ACCESS_SECRET,
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    )

    schema = Schema(
        NestedField(1, "doctor_id", LongType(), required=True),
        NestedField(2, "Name", StringType(), required=True),
        NestedField(3, "last_name", StringType(), required=True),
        NestedField(4, "specialization", StringType(), required=True),
        NestedField(5, "phone", StringType(), required=True),
        NestedField(6, "email", StringType(), required=True),
        NestedField(7, "created_at", TimestampType(), required=True),
    )

    tbl = catalog.create_table("default.doctors", schema=schema)

    tbl.append(df)

    result = tbl.scan().to_arrow()

    print(result)

if __name__ == "__main__":
    create_bucket_if_not_exists("lakehouse")
    df = read_csv(file_path)
    upload_to_iceberg(df)