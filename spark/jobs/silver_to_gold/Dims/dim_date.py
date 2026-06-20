import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session, write_pipeline_metadata_event

GOLD = "s3a://telecom-gold"
ENDPOINT = "http://minio:9000"


def build_dim_date(spark):
    print(" Building dim_date (gold) ")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW date_series AS
        SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-12-31'), interval 1 day)) AS date_value
    """)

    df = spark.sql("""
        SELECT
            md5(date_format(date_value, 'yyyy-MM-dd')) AS date_sk,
            date_value AS full_date,
            date_format(date_value, 'yyyy-MM-dd') AS date,
            date_format(date_value, 'yyyy') AS year,
            date_format(date_value, 'MM') AS month,
            date_format(date_value, 'dd') AS day,
            date_format(date_value, 'EEEE') AS day_name,
            dayofweek(date_value) AS day_of_week,
            CASE WHEN dayofweek(date_value) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
            date_format(date_value, 'D') AS day_of_year,
            quarter(date_value) AS quarter
        FROM date_series
    """)

    cnt = df.count()
    print(f"  dim_date rows: {cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.dim_date")
    df.createOrReplaceTempView("_dd_tmp")
    spark.sql("CREATE TABLE local.gold.dim_date USING iceberg AS SELECT * FROM _dd_tmp")
    print("  Wrote to Iceberg local.dim_date")

    df.write.mode("overwrite").parquet(f"{GOLD}/dim_date/")
    print(f"  Wrote {cnt} rows to {GOLD}/dim_date/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="dim_date",
        action="build", row_count=cnt, status="success",
    )
    print(" dim_date complete ")
    return df


if __name__ == "__main__":
    spark = get_spark_session("GoldDimDate")
    build_dim_date(spark)
    spark.stop()

