import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, count, avg, sum as _sum, when, lit, to_date, expr, date_format,
    round as _round,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"

SILVER_PARQUET = {
    "payments": "Payments",
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


def build_payment_analytics(spark):
    print(" Building payment_analytics (gold) ")

    payments = read_table(spark, "payments")
    if payments is None:
        print("ERROR: No payment data available")
        return

    v = payments.filter(col("is_rejected") == False)

    result = v.groupBy(
        to_date("timestamp").alias("payment_date"),
        col("payment_method"),
    ).agg(
        count("*").alias("transaction_count"),
        count(when(col("status") == "completed", 1)).alias("successful_transactions"),
        count(when(col("status") != "completed", 1)).alias("failed_transactions"),
        _sum("payment_amount").alias("total_amount"),
    )

    result = result.withColumn(
        "avg_transaction_amount",
        _round(col("total_amount") / col("transaction_count"), 2),
    )

    result = result.withColumn(
        "success_rate",
        _round(col("successful_transactions") / col("transaction_count") * 100, 2),
    )

    result = result.fillna(0)

    result = result.withColumn("date_sk", expr("md5(date_format(payment_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "payment_date", "payment_method",
        "transaction_count", "successful_transactions",
        "failed_transactions", "total_amount",
        "avg_transaction_amount", "success_rate",
    )

    final_cnt = result.count()
    print(f"  payment_analytics final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.payment_analytics")
    result.createOrReplaceTempView("_pa_tmp")
    spark.sql("CREATE TABLE local.gold.payment_analytics USING iceberg AS SELECT * FROM _pa_tmp")
    print("  Wrote to Iceberg local.payment_analytics")

    result.write.mode("overwrite").parquet(f"{GOLD}/payment_analytics/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/payment_analytics/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="payment_analytics",
        action="build", row_count=final_cnt, status="success",
    )

    print(" payment_analytics complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldPaymentAnalytics")
    build_payment_analytics(spark)
    spark.stop()

