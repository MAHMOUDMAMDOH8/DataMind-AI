import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, count, avg, when, lit, to_date, expr, date_format, round as _round,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"

SILVER_PARQUET = {
    "tickets": "Support",
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


def build_support_analytics(spark):
    print(" Building support_analytics (gold) ")

    tickets = read_table(spark, "tickets")
    if tickets is None:
        print("ERROR: No support ticket data available")
        return

    v = tickets.filter(col("is_rejected") == False)

    result = v.groupBy(to_date("timestamp").alias("support_date")).agg(
        count("*").alias("tickets_created"),
        count(when(col("resolution_time_seconds").isNotNull() & (col("resolution_time_seconds") > 0), 1)).alias("tickets_resolved"),
        count(when(col("complaint_category").isNotNull(), 1)).alias("complaints_received"),
        _round(avg("resolution_time_seconds"), 2).alias("avg_resolution_time_seconds"),
        _round(avg("wait_time_seconds"), 2).alias("avg_wait_time_seconds"),
        _round(avg("satisfaction_score"), 2).alias("avg_satisfaction_score"),
        _round(count(when(col("escalated") == True, 1)) / count("*") * 100, 2).alias("escalation_rate"),
        _round(count(when(col("call_back_requested") == True, 1)) / count("*") * 100, 2).alias("callback_request_rate"),
        _round(count(when(col("first_call_resolution") == True, 1)) / count("*") * 100, 2).alias("first_call_resolution_rate"),
    )

    result = result.fillna(0)

    result = result.withColumn("date_sk", expr("md5(date_format(support_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "support_date",
        "tickets_created", "tickets_resolved", "complaints_received",
        "avg_resolution_time_seconds", "avg_wait_time_seconds",
        "avg_satisfaction_score", "escalation_rate",
        "callback_request_rate", "first_call_resolution_rate",
    )

    final_cnt = result.count()
    print(f"  support_analytics final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.support_analytics")
    result.createOrReplaceTempView("_sa_tmp")
    spark.sql("CREATE TABLE local.gold.support_analytics USING iceberg AS SELECT * FROM _sa_tmp")
    print("  Wrote to Iceberg local.support_analytics")

    result.write.mode("overwrite").parquet(f"{GOLD}/support_analytics/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/support_analytics/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="support_analytics",
        action="build", row_count=final_cnt, status="success",
    )

    print(" support_analytics complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldSupportAnalytics")
    build_support_analytics(spark)
    spark.stop()

