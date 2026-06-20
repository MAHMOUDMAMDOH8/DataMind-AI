import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, sum as _sum, count, avg, when, lit, coalesce, to_date,
    current_date, expr, datediff, round as _round,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"


SILVER_PARQUET = {
    "dim_user": None,
    "calls": "calls",
    "sms": "sms",
    "data_usage": "DataUsage",
    "payments": "Payments",
    "recharges": "Recharge",
    "roaming_sessions": "Roaming",
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


def agg_group(df, group_col, aggs):
    v = df.filter(col("is_rejected") == False)
    return v.groupBy(group_col).agg(*aggs)


def build_customer_360(spark):
    print(" Building customer_360 (gold) ")

    tables = list(SILVER_PARQUET.keys())
    loaded = {}
    for t in tables:
        loaded[t] = read_table(spark, t)

    if "dim_user" not in loaded:
        print("ERROR: dim_user table not found")
        return

    base = loaded["dim_user"].filter(col("is_current") == True).select(
        col("msisdn").alias("customer_id"),
        col("city"),
        col("customer_type"),
        col("activation_date"),
    )
    base_cnt = base.count()
    print(f"  Base customers: {base_cnt}")

    metrics = []
    join_key_map = {"calls": "from_phone_number", "sms": "from_phone_number"}

    for name, df in loaded.items():
        if name == "dim_user":
            continue
        jk = join_key_map.get(name, "phone_number")

        if name == "calls":
            m = agg_group(df, jk, [
                count("*").alias("total_calls"),
                avg("call_duration_seconds").alias("avg_call_duration"),
                _sum("call_duration_seconds").alias("total_call_duration"),
                _sum("amount").alias("total_call_revenue"),
            ])
        elif name == "sms":
            m = agg_group(df, jk, [
                count("*").alias("total_sms"),
                _sum("amount").alias("total_sms_revenue"),
            ])
        elif name == "payments":
            m = agg_group(df, jk, [
                count("*").alias("total_payments"),
                _sum("payment_amount").alias("total_payment_amount"),
            ])
        elif name == "recharges":
            m = agg_group(df, jk, [
                count("*").alias("total_recharges"),
                _sum("recharge_amount").alias("total_recharge_amount"),
            ])
        elif name == "data_usage":
            duration_col = "session_duration_seconds" if "session_duration_seconds" in df.columns else "duration_seconds"
            m = agg_group(df, jk, [
                _sum("data_used_mb").alias("total_data_usage_mb"),
                avg(duration_col).alias("avg_session_duration"),
                count("*").alias("total_sessions"),
            ])
        elif name == "roaming_sessions":
            m = agg_group(df, jk, [
                count("*").alias("total_roaming_events"),
                _sum(coalesce(col("roaming_charges"), col("amount"), lit(0))).alias("total_roaming_charges"),
            ])
        elif name == "tickets":
            m = agg_group(df, jk, [
                count("*").alias("total_tickets"),
            ])
        else:
            print(f"  Skipping unknown table: {name}")
            continue

        m = m.withColumnRenamed(jk, "customer_id")
        metrics.append((name, m))

    result = base
    for name, m in metrics:
        mc = m.count()
        result = result.join(m, "customer_id", "left")
        print(f"  Joined {name} ({mc} grouped rows)")

    result = result.fillna(0)

    expected_cols = [
        "total_calls", "total_sms", "total_data_usage_mb",
        "total_roaming_events", "total_roaming_charges",
        "total_payments", "total_recharges",
        "avg_call_duration", "avg_session_duration",
        "total_tickets",
        "total_call_duration", "total_sessions",
        "total_call_revenue", "total_sms_revenue",
        "total_payment_amount", "total_recharge_amount",
    ]
    for c in expected_cols:
        if c not in result.columns:
            result = result.withColumn(c, lit(0))

    result = result.withColumn(
        "total_revenue",
        col("total_call_revenue") + col("total_sms_revenue")
        + col("total_payment_amount") + col("total_recharge_amount")
        + col("total_roaming_charges")
    )

    result = result.withColumn(
        "customer_lifetime_value",
        _round(col("total_revenue") / (
            datediff(current_date(), to_date(col("activation_date"), "yyyy-MM-dd")) + 1
        ) * 365, 2)
    )

    result = result.withColumn("churn_score", lit(0))
    result = result.withColumn("last_activity_date", current_date())

    result = result.select(
        "customer_id", "city", "customer_type", "activation_date",
        "total_calls", "total_sms", "total_data_usage_mb",
        "total_roaming_events", "total_roaming_charges",
        "total_payments", "total_recharges", "total_revenue",
        "avg_call_duration", "avg_session_duration",
        "total_tickets",
        "last_activity_date", "customer_lifetime_value", "churn_score",
    )

    final_cnt = result.count()
    print(f"  customer_360 final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.customer_360")
    result.createOrReplaceTempView("_c360_tmp")
    spark.sql("CREATE TABLE local.gold.customer_360 USING iceberg AS SELECT * FROM _c360_tmp")
    print("  Wrote to Iceberg local.customer_360")

    result.write.mode("overwrite").parquet(f"{GOLD}/customer_360/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/customer_360/")

    write_pipeline_metadata_event(
        ENDPOINT,
        pipeline_stage="silver_to_gold",
        entity="customer_360",
        action="build",
        row_count=final_cnt,
        status="success",
    )

    print(" customer_360 complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldCustomer360")
    build_customer_360(spark)
    spark.stop()

