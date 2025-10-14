import datetime
from typing import Literal

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from pyarrow import Table as DataFrame

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.types import NestedField, TimestampType

class ArrowIcebergMinIO:
    def __init__(
        self
        , file_path: str
        , file_type: Literal['csv']
        , namespace: str
        , namespace_etl: str
        , table: str
        , catalog: load_catalog
        , arrow_schema: pa.Schema
        , iceberg_schema: Schema
    ) -> None:
        self.file_path = file_path
        self.namespace = namespace
        self.namespace_etl = namespace_etl
        self.table = table
        self.catalog = catalog
        self.arrow_schema = arrow_schema
        self.iceberg_schema = iceberg_schema

        if file_type not in ['csv']:
            raise ValueError("file_type must be one of 'csv'")
        self.file_type = file_type
        if self.file_type == 'csv':
            self.df = csv.read_csv(self.file_path)

        self.df = self.df.append_column("CreatedAt", pc.strptime(pa.array([datetime.datetime.now().isoformat(timespec="seconds")] * len(self.df)), format="%Y-%m-%dT%H:%M:%S", unit="us"))

    def drop_missing_data(self, field: str) -> DataFrame:
        mask = pc.invert(pc.is_null(self.df[field]))
        return self.df.filter(mask)

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
