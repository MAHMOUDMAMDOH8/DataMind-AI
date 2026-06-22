# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg 
import logging

logger = logging.getLogger(__name__)

def load_dim_agent(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_agent AS SELECT * FROM local.silver.dim_agent")

    query = """
    with dim_agent_source as (
        select 
            distinct agent_id as agent_id,
            name as name,
            department as department,
            city as city,
            status as status
        from dim_agent )
    select md5(agent_id || name || department ) as agent_sk , * from dim_agent_source
    """
    df = spark.sql(query)
    write_to_iceberg(df, "gold.dim_agent", mode="overwrite")
    logger.info("dim_agent loaded")