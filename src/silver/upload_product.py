import sys
import os
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

import argparse

from build.arrow_iceberg import ArrowIcebergMinIO
from build.schema import SCHEMA_ICEBERG, SCHEMA_ARROW

#arguments
parser = argparse.ArgumentParser()
parser.add_argument('--operation', type=str, required=False, default='upload', help='Operation to perform: upload or delete')
parser.add_argument('--namespace', type=str, required=False, default='silver', help='Namespace of the table')
parser.add_argument('--namespace_etl', type=str, required=False, default='gold', help='Namespace of the ETL table')
parser.add_argument('--file', type=str, required=False, default='../../dataset/datamart/Product.csv', help='CSV file to upload')
parser.add_argument('--table', type=str, required=False, default='products', help='Table name to upload or delete')
args = parser.parse_args()

if __name__ == "__main__":

    iceberg = ArrowIcebergMinIO(
        file_path=args.file
        , file_type='csv'
        , namespace=args.namespace
        , namespace_etl=args.namespace_etl
        , table=args.table
        , arrow_schema=SCHEMA_ARROW[args.table]
        , iceberg_schema=SCHEMA_ICEBERG[args.table]
    )

    if args.operation == 'upload':
        iceberg.upload_to_iceberg()
    elif args.operation == 'delete':
        iceberg.delete_iceberg_table()
    else:
        raise ValueError("operation must be one of 'upload' or 'delete'")