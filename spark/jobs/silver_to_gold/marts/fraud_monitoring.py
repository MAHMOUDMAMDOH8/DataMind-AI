import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_fraud_monitoring(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW calls AS SELECT * FROM local.silver.calls")
    spark.sql("CREATE OR REPLACE TEMP VIEW data_usage AS SELECT * FROM local.silver.data_usage")

    query = """
    with calls_fraud as (
        select
            from_phone_number as customer_id,
            date_trunc('day', timestamp) as fraud_date,
            count(case when is_rejected = true then 1 end) as suspicious_calls
        from calls
        group by from_phone_number, date_trunc('day', timestamp)
    ),
    data_fraud as (
        select
            phone_number as customer_id,
            date_trunc('day', timestamp) as fraud_date,
            count(case when is_rejected = true or fraud_indicator is not null then 1 end) as suspicious_sessions,
            avg(risk_score) as avg_risk_score,
            max(risk_score) as max_risk_score
        from data_usage
        group by phone_number, date_trunc('day', timestamp)
    ),
    daily_spine as (
        select customer_id, fraud_date from calls_fraud
        union
        select customer_id, fraud_date from data_fraud
    )
    select
        s.fraud_date,
        s.customer_id,
        coalesce(c.suspicious_calls, 0) as suspicious_calls,
        coalesce(d.suspicious_sessions, 0) as suspicious_sessions,
        coalesce(d.avg_risk_score, 0.0) as avg_risk_score,
        coalesce(d.max_risk_score, 0) as max_risk_score,
        (coalesce(c.suspicious_calls, 0) + coalesce(d.suspicious_sessions, 0)) as fraud_events
    from daily_spine s
    left join calls_fraud c on s.customer_id = c.customer_id and s.fraud_date = c.fraud_date
    left join data_fraud d on s.customer_id = d.customer_id and s.fraud_date = d.fraud_date
    where (coalesce(c.suspicious_calls, 0) + coalesce(d.suspicious_sessions, 0)) > 0
       or coalesce(d.max_risk_score, 0) > 0
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.fraud_monitoring", mode="overwrite")
    logger.info("fraud_monitoring loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="fraud_monitoring")
    load_fraud_monitoring(spark)
    spark.stop()
