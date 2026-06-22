import sys
sys.path.insert(0, "/home/iceberg/jobs")

# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg, get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_payment_analytics(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW payments AS SELECT * FROM local.silver.payments")

    query = """
    SELECT
        date(timestamp) as payment_date,
        md5(date_format(timestamp, 'yyyy-MM-dd')) as date_sk,
        payment_method,
        count(*) as transaction_count,
        count(CASE WHEN status = 'success' THEN 1 END) as successful_transactions,
        count(CASE WHEN status = 'failed' THEN 1 END) as failed_transactions,
        sum(CASE WHEN status = 'success' THEN payment_amount END) as total_amount,
        avg(CASE WHEN status = 'success' THEN payment_amount END) as avg_transaction_amount,
        round(count(CASE WHEN status = 'success' THEN 1 END) * 100.0 / count(*), 2) as success_rate
    FROM payments
    GROUP BY date(timestamp), payment_method, md5(date_format(timestamp, 'yyyy-MM-dd'))
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.payment_analytics", mode="overwrite")
    logger.info("payment_analytics loaded")

if __name__ == "__main__":
    spark = get_spark_session(app_name="payment_analytics")
    load_payment_analytics(spark)
    spark.stop()