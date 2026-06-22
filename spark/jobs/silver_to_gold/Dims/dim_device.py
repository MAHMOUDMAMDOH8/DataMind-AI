# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg
import logging

logger = logging.getLogger(__name__)

def load_dim_device(spark):

    spark.sql("CREATE OR REPLACE TEMP VIEW dim_device AS SELECT * FROM local.silver.dim_device")

    query = """
    with dim_device_source as (
        select 
            distinct tac as tac,
            brand as brand,
            model as model
        from dim_device )
    select md5(tac || brand ) as device_sk , * from dim_device_source
    """
    df = spark.sql(query)
    write_to_iceberg(df, "gold.dim_device", mode="overwrite")
    logger.info("dim_device loaded")