create table if not exists <CATALOG>.<SCHEMA>.dim_customer (
    customer_id bigint not null,
    first_name string,
    last_name string,
    email string,
    phone string,
    address string,
    city string,
    state string,
    zip_code string,
    country string,
    created_at timestamp,
    updated_at timestamp,
    primary key (customer_id)
)
using iceberg
tblproperties (
    format-version = '2',
    'write.format.default' = 'parquet',
    'read.split.target-size' = '134217728',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.dictionary-page-size' = '1048576',
    'write.parquet.data-page-size' = '1048576',
    'write.parquet.max-padding-size' = '8388608',
    'write.parquet.writer.version' = 'v2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '1',
    'compaction.min-input-files' = '4',
    'compaction.max-input-files' = '100',
    'compaction.file-size' = '536870912',
    'history.expire.min-snapshots-to-keep' = '1',
    'history.expire.max-snapshot-age-ms' = '604800000',
    'history.expire.snapshot-id-retention' = '10',
    'gc.enabled' = 'true',
    'gc.delete-after-commit.enabled' = 'true',
    'gc.grace-period-ms' = '604800000'
);