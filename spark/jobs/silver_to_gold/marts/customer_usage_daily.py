import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_customer_usage_daily(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW calls AS SELECT * FROM local.silver.calls")
    spark.sql("CREATE OR REPLACE TEMP VIEW data_usage AS SELECT * FROM local.silver.data_usage")
    spark.sql("CREATE OR REPLACE TEMP VIEW sms AS SELECT * FROM local.silver.sms")
    spark.sql("CREATE OR REPLACE TEMP VIEW payments AS SELECT * FROM local.silver.payments")
    spark.sql("CREATE OR REPLACE TEMP VIEW recharges AS SELECT * FROM local.silver.recharges")
    spark.sql("CREATE OR REPLACE TEMP VIEW roaming AS SELECT * FROM local.silver.roaming")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_user AS SELECT * FROM local.silver.dim_user")    
    

    query = """
    with dim_user_dedup as (
        select 
            distinct user_sk as customer_sk,
            msisdn as phone_number
        from dim_user
    ),
    calls_agg as (
        select
            from_phone_number as phone_number,
            date_trunc('day', timestamp) as date,
            count(*) as total_calls,
            sum(call_duration_seconds) as total_call_duration,
            avg(call_duration_seconds) as avg_call_duration
        from calls
        group by from_phone_number, date_trunc('day', timestamp)
    ),
    data_agg as (
        select
            phone_number,
            date_trunc('day', timestamp) as date,
            sum(data_used_mb) as total_data_usage_mb
        from data_usage
        group by phone_number, date_trunc('day', timestamp)
    ),
    sms_agg as (
        select
            from_phone_number as phone_number,
            date_trunc('day', timestamp) as date,
            count(*) as total_sms
        from sms
        group by from_phone_number, date_trunc('day', timestamp)
    ),
    payments_agg as (
        select
            phone_number,
            date_trunc('day', timestamp) as date,
            sum(amount) as total_payment_amount
        from payments
        group by phone_number, date_trunc('day', timestamp)
    ),
    recharges_agg as (
        select
            phone_number,
            date_trunc('day', timestamp) as date,
            sum(recharge_amount) as total_recharge_amount
        from recharges
        group by phone_number, date_trunc('day', timestamp)
    ),
    roaming_agg as (
        select
            phone_number,
            date_trunc('day', timestamp) as date,
            sum(data_used_mb) as total_roaming_data_usage_mb,
            count(*) as total_roaming_sessions
        from roaming
        group by phone_number, date_trunc('day', timestamp)
    ),
    daily_spine as (
        select phone_number, date from calls_agg
        union
        select phone_number, date from data_agg
        union
        select phone_number, date from sms_agg
        union
        select phone_number, date from payments_agg
        union
        select phone_number, date from recharges_agg
        union
        select phone_number, date from roaming_agg
    )
    select
        du.customer_sk,
        s.phone_number,
        s.date,
        coalesce(c.total_calls, 0) as total_calls,
        coalesce(c.total_call_duration, 0) as total_call_duration,
        coalesce(c.avg_call_duration, 0) as avg_call_duration,
        coalesce(d.total_data_usage_mb, 0) as total_data_usage_mb,
        coalesce(sm.total_sms, 0) as total_sms,
        coalesce(p.total_payment_amount, 0) as total_payment_amount,
        coalesce(r.total_recharge_amount, 0) as total_recharge_amount,
        coalesce(ro.total_roaming_data_usage_mb, 0) as total_roaming_data_usage_mb,
        coalesce(ro.total_roaming_sessions, 0) as total_roaming_sessions
    from daily_spine s
    left join dim_user_dedup du on s.phone_number = du.phone_number
    left join calls_agg c on s.phone_number = c.phone_number and s.date = c.date
    left join data_agg d on s.phone_number = d.phone_number and s.date = d.date
    left join sms_agg sm on s.phone_number = sm.phone_number and s.date = sm.date
    left join payments_agg p on s.phone_number = p.phone_number and s.date = p.date
    left join recharges_agg r on s.phone_number = r.phone_number and s.date = r.date
    left join roaming_agg ro on s.phone_number = ro.phone_number and s.date = ro.date
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.customer_usage_daily", mode="overwrite")
    logger.info("customer_usage_daily loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="customer_usage_daily")
    load_customer_usage_daily(spark)
    spark.stop()