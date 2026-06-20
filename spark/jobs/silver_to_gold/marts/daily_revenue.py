import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, sum as _sum, count, when, lit, coalesce, to_date, expr, date_format,
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
    "payments": "Payments",
    "recharges": "Recharge",
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


def build_daily_revenue(spark):
    print(" Building daily_revenue (gold) ===")

    calls = read_table(spark, "calls")
    sms_df = read_table(spark, "sms")
    payments = read_table(spark, "payments")
    recharges = read_table(spark, "recharges")
    roaming = read_table(spark, "roaming_sessions")

    parts = []

    if calls is not None:
        v = calls.filter(col("is_rejected") == False)
        a = v.groupBy(to_date("timestamp").alias("revenue_date")).agg(
            _sum("amount").alias("call_revenue"),
        )
        parts.append(a)
        print(f"  Aggregated calls")

    if sms_df is not None:
        v = sms_df.filter(col("is_rejected") == False)
        a = v.groupBy(to_date("timestamp").alias("revenue_date")).agg(
            _sum("amount").alias("sms_revenue"),
        )
        parts.append(a)
        print(f"  Aggregated sms")

    if payments is not None:
        v = payments.filter(col("is_rejected") == False)
        a = v.groupBy(to_date("timestamp").alias("revenue_date")).agg(
            _sum("payment_amount").alias("payment_revenue"),
            count(when(col("status") == "completed", 1)).alias("successful_payments"),
            count(when(col("status") != "completed", 1)).alias("failed_payments"),
        )
        parts.append(a)
        print(f"  Aggregated payments")

    if recharges is not None:
        v = recharges.filter(col("is_rejected") == False)
        a = v.groupBy(to_date("timestamp").alias("revenue_date")).agg(
            _sum("recharge_amount").alias("recharge_revenue"),
        )
        parts.append(a)
        print(f"  Aggregated recharges")

    if roaming is not None:
        v = roaming.filter(col("is_rejected") == False)
        a = v.groupBy(to_date("timestamp").alias("revenue_date")).agg(
            _sum(coalesce(col("roaming_charges"), col("amount"), lit(0))).alias("roaming_revenue"),
        )
        parts.append(a)
        print(f"  Aggregated roaming")

    if not parts:
        print("ERROR: No source data available")
        return

    result = parts[0]
    for df in parts[1:]:
        result = result.join(df, "revenue_date", "full")

    result = result.fillna(0)

    expected = ["call_revenue", "sms_revenue", "payment_revenue",
                 "roaming_revenue", "recharge_revenue",
                 "successful_payments", "failed_payments"]
    for c in expected:
        if c not in result.columns:
            result = result.withColumn(c, lit(0))

    result = result.withColumn("total_revenue", col("call_revenue") + col("sms_revenue") + col("payment_revenue") + col("roaming_revenue") + col("recharge_revenue"))

    all_phone = spark.sql("SELECT COUNT(DISTINCT msisdn) AS cnt FROM local.dim_user WHERE is_current = True").collect()[0][0]
    result = result.withColumn("active_customers", lit(all_phone))

    result = result.withColumn("date_sk", expr("md5(date_format(revenue_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "revenue_date",
        "call_revenue", "sms_revenue", "roaming_revenue",
        "payment_revenue", "recharge_revenue", "total_revenue",
        "successful_payments", "failed_payments", "active_customers",
    )

    final_cnt = result.count()
    print(f"  daily_revenue final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.daily_revenue")
    result.createOrReplaceTempView("_dr_tmp")
    spark.sql("CREATE TABLE local.gold.daily_revenue USING iceberg AS SELECT * FROM _dr_tmp")
    print("  Wrote to Iceberg local.daily_revenue")

    result.write.mode("overwrite").parquet(f"{GOLD}/daily_revenue/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/daily_revenue/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="daily_revenue",
        action="build", row_count=final_cnt, status="success",
    )

    print(" daily_revenue complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldDailyRevenue")
    build_daily_revenue(spark)
    spark.stop()

