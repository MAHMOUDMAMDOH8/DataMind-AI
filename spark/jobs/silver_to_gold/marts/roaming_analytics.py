import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_roaming_analytics(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW roaming AS SELECT * FROM local.silver.roaming")

    query = """
    SELECT
        date(timestamp) as roaming_date,
        md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
        roaming_country,
        roaming_operator,
        roaming_type,
        count(*) as roaming_events,
        count(distinct phone_number) as unique_customers,
        sum(duration_seconds) as total_duration_seconds,
        sum(data_used_mb) as total_data_used_mb,
        sum(roaming_charges) as total_roaming_charges
    FROM roaming
    GROUP BY date(timestamp), md5(date_format(timestamp, 'yyyy-MM-dd')), roaming_country, roaming_operator, roaming_type
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.roaming_analytics", mode="overwrite")
    logger.info("roaming_analytics loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="roaming_analytics")
    load_roaming_analytics(spark)
    spark.stop()
