import os
import datetime
from typing import Optional, Literal

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from pyarrow import Table as DataFrame

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

class ArrowIceberg:
    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        endpoint: str = "http://localhost:9000",
        region: str = "us-east-1",
        warehouse: str = "s3://lakehouse",
        catalog_uri: str = "thrift://localhost:9083",
        namespace: str = "silver",
        table: str = "customer",
        partition_mode: Literal["created_at_day", "ingest_dt"] = "created_at_day",
        io_impl: Literal["pyarrow", "hadoop"] = "pyarrow",
        create_namespace_if_missing: bool = True,
        format_version: str = "2",
    ) -> None:
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.endpoint = endpoint
        self.region = region

        # iceberg / hive
        self.warehouse = warehouse
        self.catalog_uri = catalog_uri
        self.namespace = namespace
        self.table = table
        self.partition_mode = partition_mode
        self.io_impl = io_impl
        self.create_namespace_if_missing = create_namespace_if_missing
        self.format_version = format_version

        # set env so PyArrow S3 picks creds
        os.environ.setdefault("AWS_ACCESS_KEY_ID", self.access_key or "")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", self.secret_key or "")
        os.environ.setdefault("AWS_DEFAULT_REGION", self.region)

        # build catalog once
        self.catalog = self._build_catalog()
