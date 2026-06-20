import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session, write_pipeline_metadata_event

GOLD = "s3a://telecom-gold"
ENDPOINT = "http://minio:9000"


def build_dim_time(spark):
    print(" Building dim_time (gold) ")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW time_series AS
        SELECT explode(sequence(0, 1439, 1)) AS minute_of_day
    """)

    df = spark.sql("""
        SELECT
            minute_of_day AS time_key,
            CAST(minute_of_day AS STRING) AS time_value,
            LPAD(CAST(FLOOR(minute_of_day / 60) AS STRING), 2, '0') AS hour,
            LPAD(CAST(MOD(minute_of_day, 60) AS STRING), 2, '0') AS minute,
            CASE WHEN FLOOR(minute_of_day / 60) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
            LPAD(CAST(FLOOR(minute_of_day / 60) AS STRING), 2, '0') || ':' || LPAD(CAST(MOD(minute_of_day, 60) AS STRING), 2, '0') AS time_24h
        FROM time_series
    """)

    cnt = df.count()
    print(f"  dim_time rows: {cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.dim_time")
    df.createOrReplaceTempView("_dt_tmp")
    spark.sql("CREATE TABLE local.gold.dim_time USING iceberg AS SELECT * FROM _dt_tmp")
    print("  Wrote to Iceberg local.dim_time")

    df.write.mode("overwrite").parquet(f"{GOLD}/dim_time/")
    print(f"  Wrote {cnt} rows to {GOLD}/dim_time/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="dim_time",
        action="build", row_count=cnt, status="success",
    )
    print(" dim_time complete ")
    return df


if __name__ == "__main__":
    spark = get_spark_session("GoldDimTime")
    build_dim_time(spark)
    spark.stop()

