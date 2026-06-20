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
    "network_metrics": "network_metrics",
    "network_qos_reports": "network_qos_reports",
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


def build_network_performance(spark):
    print("=== Building network_performance ===")

    metrics = read_table(spark, "network_metrics")
    qos = read_table(spark, "network_qos_reports")

    combined = None
    if metrics is not None:
        combined = metrics.filter(col("is_rejected") == False)
    if qos is not None:
        q = qos.filter(col("is_rejected") == False)
        combined = q if combined is None else combined.unionByName(q, allowMissingColumns=True)

    if combined is None:
        print("ERROR: No network source data available")
        return

    result = combined.groupBy(
        to_date("timestamp").alias("performance_date"),
        col("cell_site_id"),
        col("city"),
        col("region"),
        col("network_type"),
    ).agg(
        avg("active_subscribers").alias("avg_active_subscribers"),
        avg("throughput_mbps_avg").alias("avg_throughput_mbps"),
        avg("cpu_utilization_pct").alias("avg_cpu_utilization_pct"),
        avg("memory_utilization_pct").alias("avg_memory_utilization_pct"),
        avg("mos_score_avg").alias("avg_mos_score"),
        avg("jitter_ms_avg").alias("avg_jitter_ms"),
        avg("packet_loss_pct_avg").alias("avg_packet_loss_pct"),
        avg("latency_ms_avg").alias("avg_latency_ms"),
        count("*").alias("sample_count"),
    )

    result = result.fillna(0)

    result = result.withColumn(
        "network_health_score",
        _round(
            (
                col("avg_mos_score") / 5.0 * 40
                + when(col("avg_jitter_ms") < 30, 20)
                  .when(col("avg_jitter_ms") < 60, 10)
                  .otherwise(0)
                + when(col("avg_packet_loss_pct") < 1, 20)
                  .when(col("avg_packet_loss_pct") < 3, 10)
                  .otherwise(0)
                + when(col("avg_latency_ms") < 50, 20)
                  .when(col("avg_latency_ms") < 100, 10)
                  .otherwise(0)
            ), 2
        )
    )

    result = result.withColumn("date_sk", expr("md5(date_format(performance_date, 'yyyy-MM-dd'))"))

    result = result.select(
        "date_sk", "performance_date",
        "cell_site_id", "city", "region", "network_type",
        "avg_active_subscribers", "avg_throughput_mbps",
        "avg_cpu_utilization_pct", "avg_memory_utilization_pct",
        "avg_mos_score", "avg_jitter_ms", "avg_packet_loss_pct",
        "avg_latency_ms", "network_health_score",
    )

    final_cnt = result.count()
    print(f"  network_performance final rows: {final_cnt}")

    spark.sql("DROP TABLE IF EXISTS local.gold.network_performance")
    result.createOrReplaceTempView("_np_tmp")
    spark.sql("CREATE TABLE local.gold.network_performance USING iceberg AS SELECT * FROM _np_tmp")
    print("  Wrote to Iceberg local.network_performance")

    result.write.mode("overwrite").parquet(f"{GOLD}/network_performance/")
    print(f"  Wrote {final_cnt} rows to {GOLD}/network_performance/")

    write_pipeline_metadata_event(
        ENDPOINT, pipeline_stage="silver_to_gold", entity="network_performance",
        action="build", row_count=final_cnt, status="success",
    )

    print("=== network_performance complete ===")
    return result


if __name__ == "__main__":
    spark = get_spark_session("GoldNetworkPerformance")
    build_network_performance(spark)
    spark.stop()

