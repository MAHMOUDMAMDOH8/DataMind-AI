from datetime import datetime, timedelta

SPARK_CONTAINER = "spark-iceberg"
SPARK_MASTER = "local[*]"
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:s3:2.24.8"
JOBS_DIR = "/home/iceberg/jobs"

DOCKER_SPARK_SUBMIT = (
    "docker exec {container} spark-submit "
    "--master {master} "
    "--packages {packages} "
    "--conf spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
    "--conf spark.sql.catalog.local.s3.endpoint=http://minio:9000 "
    "--conf spark.sql.catalog.local.s3.path-style-access=true "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "{job_path}"
)

DOCKER_SPARK_SUBMIT_SIMPLE = (
    "docker exec {container} spark-submit --master {master} {job_path}"
)

DOCKER_EXEC = "docker exec {container} {command}"

DEFAULT_ARGS = {
    "owner": "datamind",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCHEDULE_BRONZE_TO_SILVER = "*/30 * * * *"
SCHEDULE_SILVER_TO_GOLD = "0 2 * * *"
SCHEDULE_DATA_INGESTION = None
SCHEDULE_MAINTENANCE = "0 4 * * 0"

BRONZE_TO_SILVER_MAIN = f"{JOBS_DIR}/bronze_to_silver/main.py"
SILVER_TO_GOLD_DIMS_MAIN = f"{JOBS_DIR}/silver_to_gold/Dims/main.py"
SILVER_TO_GOLD_MARTS_SCRIPT = f"{JOBS_DIR}/silver_to_gold/run_all_marts.sh"
LOAD_DIMS_SCRIPT = f"{JOBS_DIR}/load_dims.py"
