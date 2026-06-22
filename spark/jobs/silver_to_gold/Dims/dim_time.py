# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg
import logging

logger = logging.getLogger(__name__)

def load_dim_time(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW time_series AS
        SELECT 
            explode(sequence(0, 1439, 1)) AS minute_of_day
    """)

    query = """
        SELECT
            minute_of_day AS time_key,
            CAST(minute_of_day AS STRING) AS time_value,
            CAST(FLOOR(minute_of_day / 60) AS STRING) AS hour,
            CAST(MOD(minute_of_day, 60) AS STRING) AS minute,
            CASE 
                WHEN FLOOR(minute_of_day / 60) < 12 THEN 'AM'
                ELSE 'PM'
            END AS am_pm
        FROM time_series
    """
    df = spark.sql(query)
    # wite to gold iceberg
    write_to_iceberg(df, "gold.dim_time", mode="overwrite")
    logger.info("dim_time loaded")

