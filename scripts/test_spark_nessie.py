from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("TestNessie")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "nessie")
    .config("spark.sql.catalog.local.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.local.ref", "main")
    .config("spark.sql.catalog.local.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.local.s3.path-style-access", "true")
    .config("spark.sql.catalog.local.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.local.s3.secret-access-key", "minioadmin123")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

tables = spark.sql("SHOW TABLES IN local").collect()
print(f"Found {len(tables)} tables in local catalog:")
for t in tables:
    print(f"  {t.namespace}.{t.tableName}")

spark.stop()
