import datetime
import os
from typing import Literal

from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from pyarrow import Table as DataFrame

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

path_env = '../../.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'admin')
ACCESS_SECRET = os.getenv('MINIO_ROOT_PASSWORD', 'admin12345')

class ArrowIcebergMinIO:
    def __init__(
        self
        , file_path: str
        , file_type: Literal['csv']
        , namespace: str
        , namespace_etl: str
        , table: str
        , access_key: str = ACCESS_KEY
        , access_secret: str = ACCESS_SECRET
        , arrow_schema: pa.Schema = None
        , iceberg_schema: Schema = None
    ) -> None:
        self.file_path = file_path
        self.namespace = namespace
        self.namespace_etl = namespace_etl
        self.table = table
        self.arrow_schema = arrow_schema
        self.catalog = load_catalog(
            "hive",
            **{
                "uri": "thrift://localhost:9083",
                "warehouse": "s3a://lakehouse",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": access_key,
                "s3.secret-access-key": access_secret,
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
                "s3.ssl.enabled": "false",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
            }
        )
        self.iceberg_schema = iceberg_schema

        if file_type in ['csv']:
            self.df = csv.read_csv(self.file_path)
        else:
            raise ValueError("file_type must be one of 'csv'")

        self.df = self.df.append_column("CreatedAt", pc.strptime(pa.array([datetime.datetime.now().isoformat(timespec="seconds")] * len(self.df)), format="%Y-%m-%dT%H:%M:%S", unit="us"))

    def upload_to_iceberg(self) -> None:

        self.df = self.df.cast(self.arrow_schema)

        try:
            self.catalog.create_namespace(self.namespace)
        except:
            pass

        try:
            self.catalog.create_namespace(self.namespace_etl)
        except:
            pass
        
        try:
            created_id = self.iceberg_schema.find_field("CreatedAt").field_id
            partition_silver = PartitionSpec(
                PartitionField(field_id=1000, source_id=created_id, transform=DayTransform(), name="created_at_day")
            )
            tbl = self.catalog.create_table(f"{self.namespace}.{self.table}", schema=self.iceberg_schema, partition_spec=partition_silver)
        except:
            # self.catalog.drop_table(f"{self.namespace}.{self.table}")
            # created_id = self.iceberg_schema.find_field("CreatedAt").field_id
            # partition_silver = PartitionSpec(
            #     PartitionField(field_id=1000, source_id=created_id, transform=DayTransform(), name="created_at_day")
            # )
            # tbl = self.catalog.create_table(f"{self.namespace}.{self.table}", schema=self.iceberg_schema, partition_spec=partition_silver)

            tbl = self.catalog.load_table(f"{self.namespace}.{self.table}")
        
        current_date_str =  datetime.datetime.now().date().strftime("%Y-%m-%d")

        try:
            tbl.delete(f"created_at_day = '{current_date_str}'")
            print(f"Đã xóa dữ liệu ngày {current_date_str}")
        except Exception as e:
            print(f"Không thể xóa dữ liệu ngày {current_date_str}: {e}")

        try:
            tbl.overwrite(self.df)
            print(f"Đã overwrite dữ liệu ngày {current_date_str} với {len(self.df)} records")
        except Exception as e:
            print(f"Partition chưa tồn tại, append mới: {e}")
            tbl.append(self.df)
            print(f"Đã append {len(self.df)} records cho ngày {current_date_str}")

        # tbl.append(self.df)
        result = tbl.scan().to_arrow()
        print(result)

    def delete_iceberg_table(self) -> None:
        try:
            self.catalog.drop_table(f"{self.namespace}.{self.table}")
        except Exception as e:
            print(f"Error deleting table {self.table}: {e}")
