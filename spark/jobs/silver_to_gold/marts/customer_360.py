import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)


def load_customer_360(spark):
    # Register silver tables as temp views
    spark.sql("CREATE OR REPLACE TEMP VIEW crm_registration AS SELECT * FROM local.silver.crm_registration")
    spark.sql("CREATE OR REPLACE TEMP VIEW calls AS SELECT * FROM local.silver.calls")
    spark.sql("CREATE OR REPLACE TEMP VIEW sms AS SELECT * FROM local.silver.sms")
    spark.sql("CREATE OR REPLACE TEMP VIEW data_usage AS SELECT * FROM local.silver.data_usage")
    spark.sql("CREATE OR REPLACE TEMP VIEW payments AS SELECT * FROM local.silver.payments")
    spark.sql("CREATE OR REPLACE TEMP VIEW recharges AS SELECT * FROM local.silver.recharges")
    spark.sql("CREATE OR REPLACE TEMP VIEW roaming AS SELECT * FROM local.silver.roaming")
    spark.sql("CREATE OR REPLACE TEMP VIEW support_tickets AS SELECT * FROM local.silver.support_tickets")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_customer AS SELECT * FROM local.silver.dim_user")

    query = """
    WITH customer_base AS (
        SELECT DISTINCT
            customer                AS customer_id,
            phone_number,
            plan_type,
            city,
            region,
            behavior_profile,
            registration_date,
            CASE
                WHEN plan_type IN ('premium', 'enterprise') THEN 'High Value'
                WHEN plan_type IN ('business')              THEN 'Mid Value'
                ELSE 'Standard'
            END AS customer_segment
        FROM crm_registration
    ),
    dim_customer AS (
        SELECT
            distinct  user_sk as customer_sk,
            gender,
            msisdn as phone_number
        FROM dim_customer
    ),

    calls_agg AS (
        SELECT
            from_phone_number       AS phone_number,
            COUNT(*)                AS total_calls,
            AVG(call_duration_seconds) AS avg_call_duration,
            MAX(timestamp)          AS last_call_date
        FROM calls
        GROUP BY from_phone_number
    ),

    sms_agg AS (
        SELECT
            from_phone_number       AS phone_number,
            COUNT(*)                AS total_sms,
            MAX(timestamp)          AS last_sms_date
        FROM sms
        GROUP BY from_phone_number
    ),

    data_agg AS (
        SELECT
            phone_number,
            SUM(data_used_mb)              AS total_data_usage_mb,
            AVG(session_duration_seconds)  AS avg_session_duration,
            MAX(timestamp)                 AS last_data_date
        FROM data_usage
        GROUP BY phone_number
    ),

    roaming_agg AS (
        SELECT
            phone_number,
            COUNT(*)                AS total_roaming_events,
            SUM(roaming_charges)    AS total_roaming_charges,
            MAX(timestamp)          AS last_roaming_date
        FROM roaming
        GROUP BY phone_number
    ),

    payments_agg AS (
        SELECT
            phone_number,
            COUNT(*)                AS total_payments,
            SUM(payment_amount)     AS total_payment_amount,
            MAX(timestamp)          AS last_payment_date
        FROM payments
        GROUP BY phone_number
    ),

    recharges_agg AS (
        SELECT
            phone_number,
            COUNT(*)                AS total_recharges,
            SUM(recharge_amount)    AS total_recharge_amount,
            MAX(timestamp)          AS last_recharge_date
        FROM recharges
        GROUP BY phone_number
    ),

    tickets_agg AS (
        SELECT
            phone_number,
            COUNT(*)                                    AS total_tickets,
            SUM(CASE WHEN reason LIKE '%complaint%' 
                      OR priority = 'high' THEN 1 ELSE 0 END) AS total_complaints,
            MAX(timestamp)                              AS last_ticket_date
        FROM support_tickets
        GROUP BY phone_number
    )

    SELECT
        dc.customer_sk,
        dc.gender,
        dc.phone_number,
        cb.plan_type,
        cb.city,
        cb.region,
        cb.behavior_profile,
        cb.registration_date,
        cb.customer_segment,

        COALESCE(ca.total_calls, 0)             AS total_calls,
        COALESCE(sa.total_sms, 0)               AS total_sms,
        COALESCE(da.total_data_usage_mb, 0)     AS total_data_usage_mb,
        COALESCE(ro.total_roaming_events, 0)    AS total_roaming_events,
        COALESCE(ro.total_roaming_charges, 0)   AS total_roaming_charges,
        COALESCE(pa.total_payments, 0)          AS total_payments,
        COALESCE(re.total_recharges, 0)         AS total_recharges,

        -- total_revenue = payments + recharges + roaming charges
        COALESCE(pa.total_payment_amount, 0) 
            + COALESCE(re.total_recharge_amount, 0) 
            + COALESCE(ro.total_roaming_charges, 0) AS total_revenue,

        COALESCE(ca.avg_call_duration, 0)       AS avg_call_duration,
        COALESCE(da.avg_session_duration, 0)    AS avg_session_duration,

        COALESCE(ta.total_tickets, 0)           AS total_tickets,
        COALESCE(ta.total_complaints, 0)        AS total_complaints,

        -- last_activity_date = most recent event across all sources
        GREATEST(
            COALESCE(ca.last_call_date,    '1970-01-01'),
            COALESCE(sa.last_sms_date,     '1970-01-01'),
            COALESCE(da.last_data_date,    '1970-01-01'),
            COALESCE(ro.last_roaming_date, '1970-01-01'),
            COALESCE(pa.last_payment_date, '1970-01-01'),
            COALESCE(re.last_recharge_date,'1970-01-01'),
            COALESCE(ta.last_ticket_date,  '1970-01-01')
        ) AS last_activity_date,

        -- customer_lifetime_value = total revenue * usage engagement factor
        ROUND(
            (COALESCE(pa.total_payment_amount, 0) 
             + COALESCE(re.total_recharge_amount, 0) 
             + COALESCE(ro.total_roaming_charges, 0))
            * (1 + LOG(1 + COALESCE(ca.total_calls, 0) + COALESCE(sa.total_sms, 0)) * 0.1),
        2) AS customer_lifetime_value,

        -- churn_score: higher = more likely to churn (0-100)
        ROUND(
            LEAST(100, GREATEST(0,
                50
                - (COALESCE(ca.total_calls, 0) + COALESCE(sa.total_sms, 0)) * 0.5
                - COALESCE(da.total_data_usage_mb, 0) * 0.01
                + COALESCE(ta.total_complaints, 0) * 10
                + COALESCE(ta.total_tickets, 0) * 3
                - COALESCE(pa.total_payments, 0) * 2
                - COALESCE(re.total_recharges, 0) * 2
            )),
        2) AS churn_score

    FROM customer_base cb
    LEFT JOIN dim_customer dc ON cb.phone_number = dc.phone_number
    LEFT JOIN calls_agg     ca ON cb.phone_number = ca.phone_number
    LEFT JOIN sms_agg       sa ON cb.phone_number = sa.phone_number
    LEFT JOIN data_agg      da ON cb.phone_number = da.phone_number
    LEFT JOIN roaming_agg   ro ON cb.phone_number = ro.phone_number
    LEFT JOIN payments_agg  pa ON cb.phone_number = pa.phone_number
    LEFT JOIN recharges_agg re ON cb.phone_number = re.phone_number
    LEFT JOIN tickets_agg   ta ON cb.phone_number = ta.phone_number
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.customer_360", mode="overwrite")
    logger.info("customer_360 loaded")


if __name__ == "__main__":
    spark = get_spark_session("GoldCustomer360")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    load_customer_360(spark)
    spark.stop()

