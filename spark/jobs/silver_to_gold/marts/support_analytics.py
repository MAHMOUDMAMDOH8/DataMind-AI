import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_support_analytics(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW support_tickets AS SELECT * FROM local.silver.support_tickets")

    query = """
    SELECT
        date(timestamp) as support_date,    
        md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
        count(*) as tickets_created,
        count(CASE WHEN event_type = 'ticket_resolved' THEN 1 END) as tickets_resolved,
        count(CASE WHEN event_type = 'complaint_filed' THEN 1 END) as complaints_received,
        avg(CASE WHEN event_type = 'ticket_resolved' THEN cast(resolution_time_seconds as double) END) as avg_resolution_time_seconds,
        avg(CASE WHEN event_type = 'ticket_resolved' THEN cast(wait_time_seconds as double) END) as avg_wait_time_seconds,
        avg(CASE WHEN event_type = 'ticket_resolved' THEN cast(satisfaction_score as double) END) as avg_satisfaction_score,
        count(CASE WHEN event_type = 'ticket_resolved' AND escalated = true THEN 1 END) * 100.0 /
            nullif(count(CASE WHEN event_type = 'ticket_resolved' THEN 1 END), 0) as escalation_rate,
        count(CASE WHEN event_type = 'ticket_resolved' AND call_back_requested = true THEN 1 END) * 100.0 /
            nullif(count(CASE WHEN event_type = 'ticket_resolved' THEN 1 END), 0) as callback_request_rate,
        count(CASE WHEN event_type = 'ticket_resolved' AND first_call_resolution = true THEN 1 END) * 100.0 /
            nullif(count(CASE WHEN event_type = 'ticket_resolved' THEN 1 END), 0) as first_call_resolution_rate
    FROM support_tickets
    GROUP BY date(timestamp), md5(date_format(timestamp, 'yyyy-MM-dd'))
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.support_analytics", mode="overwrite")
    logger.info("support_analytics loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="support_analytics")
    load_support_analytics(spark)
    spark.stop()
