import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_network_performance(spark):

    spark.sql("CREATE OR REPLACE TEMP VIEW network_qos_reports AS SELECT * FROM local.silver.network_qos_reports")
    spark.sql("CREATE OR REPLACE TEMP VIEW network_metrics AS SELECT * FROM local.silver.network_metrics")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_cell_site AS SELECT * FROM local.silver.dim_cell_site")

    query = """
    WITH metrics_daily AS (
        SELECT 
            date(timestamp) as performance_date,
            cell_site_id,
            city,
            region,
            network_type,
            avg(active_subscribers) as avg_active_subscribers,
            avg(total_throughput_mbps) as avg_throughput_mbps_metrics,
            avg(cpu_utilization_pct) as avg_cpu_utilization_pct,
            avg(memory_utilization_pct) as avg_memory_utilization_pct
        FROM network_metrics
        GROUP BY 1, 2, 3, 4, 5
    ),
    qos_daily AS (
        SELECT 
            date(timestamp) as performance_date,
            cell_site_id,
            city,
            region,
            network_type,
            avg(mos_score_avg) as avg_mos_score,
            avg(jitter_ms_avg) as avg_jitter_ms,
            avg(packet_loss_pct_avg) as avg_packet_loss_pct,
            avg(latency_ms_avg) as avg_latency_ms,
            avg(throughput_mbps_avg) as avg_throughput_mbps_qos
        FROM network_qos_reports
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT 
        COALESCE(m.performance_date, q.performance_date) as performance_date,
        COALESCE(m.cell_site_id, q.cell_site_id) as cell_site_id,
        COALESCE(m.city, q.city) as city,
        COALESCE(m.region, q.region) as region,
        COALESCE(m.network_type, q.network_type) as network_type,
        m.avg_active_subscribers,
        COALESCE(m.avg_throughput_mbps_metrics, q.avg_throughput_mbps_qos) as avg_throughput_mbps,
        m.avg_cpu_utilization_pct,
        m.avg_memory_utilization_pct,
        q.avg_mos_score,
        q.avg_jitter_ms,
        q.avg_packet_loss_pct,
        q.avg_latency_ms,
        -- Simple derived health score: Base score from MOS, minus penalties for packet loss and CPU usage
        ((COALESCE(q.avg_mos_score, 0) / 5.0) * 100.0) - COALESCE(q.avg_packet_loss_pct, 0.0) - (COALESCE(m.avg_cpu_utilization_pct, 0.0) * 0.1) as network_health_score
    FROM metrics_daily m
    FULL OUTER JOIN qos_daily q
        ON m.performance_date = q.performance_date
        AND m.cell_site_id = q.cell_site_id
        AND m.city = q.city
        AND m.region = q.region
        AND m.network_type = q.network_type
    """
    
    df = spark.sql(query)
    write_to_iceberg(df, "gold.network_performance", mode="overwrite")
    logger.info("network_performance loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="DataMind_Network_Performance")
    load_network_performance(spark)
    spark.stop()