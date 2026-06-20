import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, when, lit, to_date, expr, date_format,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"

SILVER_PARQUET = {
    "calls": "calls",
    "data_usage": "DataUsage",
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


def build_fraud_monitoring(spark):
    print(" Building fraud_monitoring (gold) ")

    calls = read_table(spark, "calls")
    data = read_table(spark, "data_usage")

    parts = []

    if calls is not None:
        v = calls.filter(col("is_rejected") == False)
        suspicious = v.filter(
            (col("status").isin("failed", "no-answer", "busy"))
            | (col("amount") > 100)
        )
        a = suspicious.groupBy(
            to_date("timestamp").alias("fraud_date"),
            col("from_phone_number").alias("customer_id"),
        ).agg(
            count("*").alias("suspicious_calls"),
        )
        parts.append(a)
        print(f"  Aggregated suspicious calls")

    if data is not None:
        v = data.filter(col("is_rejected") == False)
        a = v.groupBy(
            to_date("timestamp").alias("fraud_date"),
            col("phone_number").alias("customer_id"),
        ).agg(
            count(when(col("fraud_indicator").isNotNull(), 1)).alias("suspicious_sessions"),
            avg("risk_score").alias("avg_risk_score"),
            _max("risk_score").alias("max_risk_score"),
            count("*").alias("fraud_events"),
        )
        parts.append(a)
        print(f"  Aggregated data_usage fraud metrics")

    if not parts:
        print("ERROR: No source data available")
        return

    result = parts[0]
    for df in parts[1:]:
        result = result.join(df, ["fraud_date", "customer_id"], "full")

    result = result.fillna(0)

    expected = ["suspicious_calls", "suspicious_sessions",
                 "avg_risk_score", "max_risk_score", "fraud_events"]
    for c in expected:
        if c not in result.columns:
            result = result.withColumn(c, lit(0))

    result = result.withColumn("date_sk", expr("md5(date_format(fraud_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "fraud_date", "customer_id",
        "suspicious_calls", "suspicious_sessions",
        "avg_risk_score", "max_risk_score", "fraud_events",
    )

    final_cnt = result.count()
    print(f"  fraud_monitoring final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.fraud_monitoring")
    result.createOrReplaceTempView("_fm_tmp")
    spark.sql("CREATE TABLE local.gold.fraud_monitoring USING iceberg AS SELECT * FROM _fm_tmp")
    print("  Wrote to Iceberg local.fraud_monitoring")

    result.write.mode("overwrite").parquet(f"{GOLD}/fraud_monitoring/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/fraud_monitoring/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="fraud_monitoring",
        action="build", row_count=final_cnt, status="success",
    )

    print(" fraud_monitoring complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldFraudMonitoring")
    build_fraud_monitoring(spark)
    spark.stop()

