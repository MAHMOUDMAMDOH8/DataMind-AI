import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session, read_from_iceberg, write_pipeline_metadata_event

GOLD = "s3a://telecom-gold"
ENDPOINT = "http://minio:9000"


def build_dim_cell_site(spark):
    print(" Building dim_cell_site (gold) ")

    df = read_from_iceberg("dim_cell_site", spark)
    if df is None:
        print("ERROR: dim_cell_site not found in Iceberg")
        return

    result = df.filter("is_current = true").select(
        "cell_sk", "cell_id", "site_name", "city",
        "latitude", "longitude",
    )

    cnt = result.count()
    print(f"  dim_cell_site rows: {cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.dim_cell_site_gold")
    result.createOrReplaceTempView("_dcs_tmp")
    spark.sql("CREATE TABLE local.gold.dim_cell_site_gold USING iceberg AS SELECT * FROM _dcs_tmp")
    print("  Wrote to Iceberg local.dim_cell_site_gold")

    result.write.mode("overwrite").parquet(f"{GOLD}/dim_cell_site/")
    print(f"  Wrote {cnt} rows to {GOLD}/dim_cell_site/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="dim_cell_site",
        action="build", row_count=cnt, status="success",
    )
    print(" dim_cell_site complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldDimCellSite")
    build_dim_cell_site(spark)
    spark.stop()

