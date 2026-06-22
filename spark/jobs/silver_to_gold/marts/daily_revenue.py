import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_daily_revenue(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW payments AS SELECT * FROM local.silver.payments")
    spark.sql("CREATE OR REPLACE TEMP VIEW recharges AS SELECT * FROM local.silver.recharges")
    spark.sql("CREATE OR REPLACE TEMP VIEW roaming AS SELECT * FROM local.silver.roaming")
    spark.sql("CREATE OR REPLACE TEMP VIEW sms AS SELECT * FROM local.silver.sms")
    spark.sql("CREATE OR REPLACE TEMP VIEW calls AS SELECT * FROM local.silver.calls")

    query = """
    with payments_agg as (
        select
            date_trunc('day', timestamp) as date,
            md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
            sum(amount) as total_payment_amount
        from payments
        group by date_trunc('day', timestamp),md5(date_format(timestamp, 'yyyy-MM-dd'))
    ),
    recharges_agg as (
        select
            date_trunc('day', timestamp) as date,
            md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
            sum(recharge_amount) as total_recharge_amount
        from recharges
        group by date_trunc('day', timestamp),md5(date_format(timestamp, 'yyyy-MM-dd'))
    ),
    roaming_agg as (
        select
            date_trunc('day', timestamp) as date,
            md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
            sum(roaming_charges) as total_roaming_charges
        from roaming
        group by date_trunc('day', timestamp),md5(date_format(timestamp, 'yyyy-MM-dd'))
    ),
    sms_agg as (
        select
            date_trunc('day', timestamp) as date,
            md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
            sum(amount) as total_sms_charges
        from sms
        group by date_trunc('day', timestamp),md5(date_format(timestamp, 'yyyy-MM-dd'))
    ),
    calls_agg as (
        select
            date_trunc('day', timestamp) as date,
            md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
            sum(amount) as total_call_charges
        from calls
        group by date_trunc('day', timestamp),md5(date_format(timestamp, 'yyyy-MM-dd'))
    ),
    daily_spine as (
        select date,date_sk from payments_agg
        union
        select date,date_sk from recharges_agg
        union
        select date,date_sk from roaming_agg
        union
        select date,date_sk from sms_agg
        union
        select date,date_sk from calls_agg
    )
    select
        s.date_sk,
        s.date,
        coalesce(p.total_payment_amount, 0) + coalesce(r.total_recharge_amount, 0) + coalesce(ro.total_roaming_charges, 0) as total_revenue,
        coalesce(p.total_payment_amount, 0) as total_payment_amount,
        coalesce(r.total_recharge_amount, 0) as total_recharge_amount,
        coalesce(ro.total_roaming_charges, 0) as total_roaming_charges,
        coalesce(sm.total_sms_charges, 0) as total_sms_charges,
        coalesce(ca.total_call_charges, 0) as total_call_charges
    from daily_spine s
    left join payments_agg p using (date)
    left join recharges_agg r using (date)
    left join roaming_agg ro using (date)
    left join sms_agg sm using (date)
    left join calls_agg ca using (date)
    order by s.date
    """ 

    df = spark.sql(query)
    write_to_iceberg(df, "gold.daily_revenue", mode="overwrite")
    logger.info("daily_revenue loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="daily_revenue")
    load_daily_revenue(spark)
    spark.stop()
