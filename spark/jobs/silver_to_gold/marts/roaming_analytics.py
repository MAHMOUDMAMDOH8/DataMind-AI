import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, count, sum as _sum, to_date, expr, date_format, countDistinct,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"

SILVER_PARQUET = {
    "roaming_sessions": "Roaming",
}


def read_table(spark, name):
    df = read_from_iceberg(name, spark)
    if df is not None:
        return df
    pq_path = SILVER_PARQUET.get(name)
    if pq_path is not None:
        try:
            df = spark.read.parquet(f"{SILVER}/{pq_path}/")
            cnt = df.count()
            print(f"  {name}: {cnt} rows (from Parquet)")
            return df
        except Exception:
            pass
    print(f"  {name}: NOT FOUND")
    return None


def build_roaming_analytics(spark):
    print(" Building roaming_analytics (gold) ")

    roaming = read_table(spark, "roaming_sessions")
    if roaming is None:
        print("ERROR: No roaming data available")
        return

    v = roaming.filter(col("is_rejected") == False)

    result = v.groupBy(
        to_date("timestamp").alias("roaming_date"),
        col("roaming_country"),
        col("roaming_operator"),
        col("roaming_type"),
    ).agg(
        count("*").alias("roaming_events"),
        countDistinct("phone_number").alias("unique_customers"),
        _sum("duration_seconds").alias("total_duration_seconds"),
        _sum("data_used_mb").alias("total_data_used_mb"),
        _sum("roaming_charges").alias("total_roaming_charges"),
    )

    result = result.fillna(0)

    result = result.withColumn("date_sk", expr("md5(date_format(roaming_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "roaming_date",
        "roaming_country", "roaming_operator", "roaming_type",
        "roaming_events", "unique_customers",
        "total_duration_seconds", "total_data_used_mb", "total_roaming_charges",
    )

    final_cnt = result.count()
    print(f"  roaming_analytics final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.roaming_analytics")
    result.createOrReplaceTempView("_ra_tmp")
    spark.sql("CREATE TABLE local.gold.roaming_analytics USING iceberg AS SELECT * FROM _ra_tmp")
    print("  Wrote to Iceberg local.roaming_analytics")

    result.write.mode("overwrite").parquet(f"{GOLD}/roaming_analytics/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/roaming_analytics/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="roaming_analytics",
        action="build", row_count=final_cnt, status="success",
    )

    print(" roaming_analytics complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldRoamingAnalytics")
    build_roaming_analytics(spark)
    spark.stop()

