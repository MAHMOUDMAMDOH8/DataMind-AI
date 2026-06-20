import sys
sys.path.insert(0, "/home/iceberg/jobs")

from pyspark.sql.functions import (
    col, count, avg, sum as _sum, to_date, expr, date_format, round as _round, countDistinct,
)

from scripts.spark_init import (
    get_spark_session, read_from_iceberg, write_pipeline_metadata_event,
)

GOLD = "s3a://telecom-gold"
SILVER = "s3a://telecom-silver"
ENDPOINT = "http://minio:9000"

SILVER_PARQUET = {
    "recharges": "Recharge",
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


def build_recharge_analytics(spark):
    print(" Building recharge_analytics (gold) ")

    recharges = read_table(spark, "recharges")
    if recharges is None:
        print("ERROR: No recharge data available")
        return

    v = recharges.filter(col("is_rejected") == False)

    result = v.groupBy(
        to_date("timestamp").alias("recharge_date"),
        col("payment_method"),
    ).agg(
        count("*").alias("recharge_count"),
        _sum("recharge_amount").alias("total_recharge_amount"),
        countDistinct("phone_number").alias("unique_customers"),
    )

    result = result.withColumn(
        "avg_recharge_amount",
        _round(col("total_recharge_amount") / col("recharge_count"), 2),
    )

    result = result.withColumn("date_sk", expr("md5(date_format(recharge_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "recharge_date", "payment_method",
        "recharge_count", "total_recharge_amount",
        "avg_recharge_amount", "unique_customers",
    )

    final_cnt = result.count()
    print(f"  recharge_analytics final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.recharge_analytics")
    result.createOrReplaceTempView("_ra_tmp")
    spark.sql("CREATE TABLE local.gold.recharge_analytics USING iceberg AS SELECT * FROM _ra_tmp")
    print("  Wrote to Iceberg local.recharge_analytics")

    result.write.mode("overwrite").parquet(f"{GOLD}/recharge_analytics/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/recharge_analytics/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="recharge_analytics",
        action="build", row_count=final_cnt, status="success",
    )

    print(" recharge_analytics complete ")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldRechargeAnalytics")
    build_recharge_analytics(spark)
    spark.stop()

