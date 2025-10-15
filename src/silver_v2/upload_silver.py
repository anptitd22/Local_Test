import sys
import os
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

from build.arrow_iceberg import ArrowIcebergMinIO
from build.schema import SCHEMA_ICEBERG, SCHEMA_ARROW

# Định nghĩa mapping các table
TABLE_CONFIG = {
    'customers': {
        'file': 'Customers.csv',
        'table': 'customers'
    },
    'products': {
        'file': 'Product.csv',
        'table': 'products'
    },
    'order_headers': {
        'file': 'OrderHeader.csv',
        'table': 'order_headers'
    },
    'order_details': {
        'file': 'OrderDetail.csv',
        'table': 'order_details'
    },
    'product_categories': {
        'file': 'ProductCategory.csv',
        'table': 'product_categories'
    },
    'product_sub_categories': {
        'file': 'ProductSubCategory.csv',
        'table': 'product_sub_categories'
    }
}

def upload_table_to_iceberg(table_key: str, 
                            data_dir: str = "/opt/airflow/dataset/datamart", 
                            operation: str = 'upload', 
                            namespace: str = 'silver', 
                            namespace_etl: str = 'gold', 
                            **context
                            ) -> str:
    
    config = TABLE_CONFIG[table_key]
    file_path = os.path.join(data_dir, config['file'])
    table_name = config['table']
    
    print(f"Processing {operation} for table: {table_name}")
    print(f"File path: {file_path}")
    print(f"Namespace: {namespace}")
    print(f"Namespace ETL: {namespace_etl}")
    
    
    try:
        iceberg = ArrowIcebergMinIO(
            file_path=file_path,
            file_type='csv',
            namespace=namespace,
            namespace_etl=namespace_etl,
            table=table_name,
            arrow_schema=SCHEMA_ARROW[table_name],
            iceberg_schema=SCHEMA_ICEBERG[table_name]
        )

        if operation == 'upload':
            iceberg.upload_to_iceberg()
            print(f"Successfully uploaded {table_name}")
            return f"Upload completed for {table_name}"
            
        elif operation == 'delete':
            iceberg.delete_iceberg_table()
            print(f"Successfully deleted {table_name}")
            return f"Delete completed for {table_name}"
            
        else:
            raise ValueError(f"Invalid operation: {operation}. Must be 'upload' or 'delete'")
            
    except Exception as e:
        print(f"Error processing table {table_name}: {str(e)}")
        raise

def upload_customer(**context):
    return upload_table_to_iceberg('customers', **context)

def upload_product(**context):
    return upload_table_to_iceberg('products', **context)

def upload_order_header(**context):
    return upload_table_to_iceberg('order_headers', **context)

def upload_order_detail(**context):
    return upload_table_to_iceberg('order_details', **context)

def upload_product_category(**context):
    return upload_table_to_iceberg('product_categories', **context)

def upload_product_sub_category(**context):
    return upload_table_to_iceberg('product_sub_categories', **context)