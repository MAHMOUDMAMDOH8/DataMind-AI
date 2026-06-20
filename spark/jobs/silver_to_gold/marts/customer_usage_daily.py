import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, sum as _sum, count, avg, when, lit, coalesce, to_date,
    current_date, expr, date_format,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"

SILVER_PARQUET = {
    "calls": "calls",
    "sms": "sms",
    "data_usage": "DataUsage",
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


def build_customer_usage_daily(spark):
    print(" Building customer_usage_daily (gold) ")

    calls = read_table(spark, "calls")
    sms_df = read_table(spark, "sms")
    data = read_table(spark, "data_usage")
    roaming = read_table(spark, "roaming_sessions")

    parts = []

    if calls is not None:
        v = calls.filter(col("is_rejected") == False)
        a = v.groupBy(
            to_date("timestamp").alias("usage_date"),
            col("from_phone_number").alias("customer_id"),
        ).agg(
            count("*").alias("calls_count"),
            _sum("call_duration_seconds").alias("total_call_duration_seconds"),
        )
        parts.append(a)
        print(f"  Aggregated calls")

    if sms_df is not None:
        v = sms_df.filter(col("is_rejected") == False)
        a = v.groupBy(
            to_date("timestamp").alias("usage_date"),
            col("from_phone_number").alias("customer_id"),
        ).agg(
            count("*").alias("sms_count"),
        )
        parts.append(a)
        print(f"  Aggregated sms")

    if data is not None:
        v = data.filter(col("is_rejected") == False)
        a = v.groupBy(
            to_date("timestamp").alias("usage_date"),
            col("phone_number").alias("customer_id"),
        ).agg(
            _sum("data_used_mb").alias("total_data_usage_mb"),
            count("*").alias("session_count"),
            avg("session_duration_seconds").alias("avg_session_duration_seconds"),
        )
        parts.append(a)
        print(f"  Aggregated data_usage")

    if roaming is not None:
        v = roaming.filter(col("is_rejected") == False)
        a = v.groupBy(
            to_date("timestamp").alias("usage_date"),
            col("phone_number").alias("customer_id"),
        ).agg(
            _sum("data_used_mb").alias("roaming_usage_mb"),
            count("*").alias("roaming_events"),
        )
        parts.append(a)
        print(f"  Aggregated roaming")

    if not parts:
        print("ERROR: No source data available")
        return

    result = parts[0]
    for df in parts[1:]:
        result = result.join(df, ["usage_date", "customer_id"], "full")

    result = result.fillna(0)

    expected = ["calls_count", "total_call_duration_seconds", "sms_count",
                 "total_data_usage_mb", "roaming_usage_mb", "roaming_events",
                 "session_count", "avg_session_duration_seconds"]
    for c in expected:
        if c not in result.columns:
            result = result.withColumn(c, lit(0))

    result = result.withColumn(
        "date_sk", expr("md5(date_format(usage_date, 'yyyy-MM-dd'))")
    )

    result = result.select(
        "date_sk", "usage_date", "customer_id",
        "calls_count", "total_call_duration_seconds", "sms_count",
        "total_data_usage_mb", "roaming_usage_mb", "roaming_events",
        "session_count", "avg_session_duration_seconds",
    )

    final_cnt = result.count()
    print(f"  customer_usage_daily final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.customer_usage_daily")
    result.createOrReplaceTempView("_cud_tmp")
    spark.sql("CREATE TABLE local.gold.customer_usage_daily USING iceberg AS SELECT * FROM _cud_tmp")
    print("  Wrote to Iceberg local.customer_usage_daily")

    result.write.mode("overwrite").parquet(f"{GOLD}/customer_usage_daily/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/customer_usage_daily/")

    write_pipeline_metadata_event(
        ENDPOINT,
        pipeline_stage="silver_to_gold",
        entity="customer_usage_daily",
        action="build",
        row_count=final_cnt,
        status="success",
    )

    print(" customer_usage_daily complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldUsageDaily")
    build_customer_usage_daily(spark)
    spark.stop()

