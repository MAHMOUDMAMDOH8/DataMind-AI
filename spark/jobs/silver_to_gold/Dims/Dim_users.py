import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session, read_from_iceberg, write_pipeline_metadata_event

GOLD = "s3a://telecom-gold"
ENDPOINT = "http://minio:9000"


def build_dim_user(spark):
    print(" Building dim_user (gold) ")

    df = read_from_iceberg("dim_user", spark)
    if df is None:
        print("ERROR: dim_user not found in Iceberg")
        return

    result = df.filter("is_current = true").select(
        "user_sk", "msisdn", "city", "customer_type",
        "gender", "age_group", "activation_date", "status",
    )

    cnt = result.count()
    print(f"  dim_user rows: {cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.dim_user_gold")
    result.createOrReplaceTempView("_du_tmp")
    spark.sql("CREATE TABLE local.gold.dim_user_gold USING iceberg AS SELECT * FROM _du_tmp")
    print("  Wrote to Iceberg local.dim_user_gold")

    result.write.mode("overwrite").parquet(f"{GOLD}/dim_user/")
    print(f"  Wrote {cnt} rows to {GOLD}/dim_user/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="dim_user",
        action="build", row_count=cnt, status="success",
    )
    print(" dim_user complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldDimUser")
    build_dim_user(spark)
    spark.stop()

