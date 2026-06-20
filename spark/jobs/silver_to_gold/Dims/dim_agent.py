import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session, read_from_iceberg, write_pipeline_metadata_event

GOLD = "s3a://telecom-gold"
ENDPOINT = "http://minio:9000"


def build_dim_agent(spark):
    print(" Building dim_agent (gold) ")

    df = read_from_iceberg("dim_agent", spark)
    if df is None:
        print("ERROR: dim_agent not found in Iceberg")
        return

    result = df.filter("is_current = true").select(
        "agent_sk", "agent_id", "name", "department",
        "city", "status",
    )

    cnt = result.count()
    print(f"  dim_agent rows: {cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.dim_agent_gold")
    result.createOrReplaceTempView("_da_tmp")
    spark.sql("CREATE TABLE local.gold.dim_agent_gold USING iceberg AS SELECT * FROM _da_tmp")
    print("  Wrote to Iceberg local.dim_agent_gold")

    result.write.mode("overwrite").parquet(f"{GOLD}/dim_agent/")
    print(f"  Wrote {cnt} rows to {GOLD}/dim_agent/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="dim_agent",
        action="build", row_count=cnt, status="success",
    )
    print(" dim_agent complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldDimAgent")
    build_dim_agent(spark)
    spark.stop()

