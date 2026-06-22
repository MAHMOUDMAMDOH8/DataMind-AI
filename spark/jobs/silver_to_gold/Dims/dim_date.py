# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg
import logging

logger = logging.getLogger(__name__)



def load_dim_date(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW date_series AS
        SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-12-31'), interval 1 day)) AS date_value
     """)
    query = """
    with date_series_source as (
        select 
            date_value as full_date,
            date_format(date_value, 'yyyy-MM-dd') as date,
            date_format(date_value, 'EEEE') AS day_name,
            dayofweek(date_value) AS day_of_week,
            CASE WHEN dayofweek(date_value) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
            date_format(date_value, 'D') AS day_of_year,
            quarter(date_value) AS quarter,
            date_format(date_value, 'yyyy') as year,
            date_format(date_value, 'MM') as month,
            date_format(date_value, 'dd') as day
        from date_series
    )
    select md5(date) as date_sk , * from date_series_source
    """
    df = spark.sql(query)
    write_to_iceberg(df, "gold.dim_date", mode="overwrite")
    logger.info("dim_date loaded")