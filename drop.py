import pyiceberg
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
import os

path_env = './.env'
load_dotenv(path_env)

ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
ACCESS_SECRET = os.getenv('MINIO_ROOT_PASSWORD')

catalog = load_catalog(
    "hive",
    **{
        "uri": "thrift://localhost:9083",
        "warehouse": "s3a://lakehouse",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": ACCESS_KEY,
        "s3.secret-access-key": ACCESS_SECRET,
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    }
)
tbl = catalog.drop_table("default.doctors")
