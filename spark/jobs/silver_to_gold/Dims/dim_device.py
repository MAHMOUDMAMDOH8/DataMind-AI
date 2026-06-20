import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session, read_from_iceberg, write_pipeline_metadata_event

GOLD = "s3a://telecom-gold"
ENDPOINT = "http://minio:9000"


def build_dim_device(spark):
    print(" Building dim_device (gold) ")

    df = read_from_iceberg("dim_device", spark)
    if df is None:
        print("ERROR: dim_device not found in Iceberg")
        return

    result = df.filter("is_current = true").select(
        "device_sk", "tac", "brand", "model",
        "os", "is_smartphone",
    )

    cnt = result.count()
    print(f"  dim_device rows: {cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.dim_device_gold")
    result.createOrReplaceTempView("_dd_tmp")
    spark.sql("CREATE TABLE local.gold.dim_device_gold USING iceberg AS SELECT * FROM _dd_tmp")
    print("  Wrote to Iceberg local.dim_device_gold")

    result.write.mode("overwrite").parquet(f"{GOLD}/dim_device/")
    print(f"  Wrote {cnt} rows to {GOLD}/dim_device/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="dim_device",
        action="build", row_count=cnt, status="success",
    )
    print(" dim_device complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldDimDevice")
    build_dim_device(spark)
    spark.stop()

