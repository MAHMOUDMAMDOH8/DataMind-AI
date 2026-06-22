# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg
import logging

logger = logging.getLogger(__name__)

def load_dim_cell_site(spark):

    spark.sql("CREATE OR REPLACE TEMP VIEW dim_cell_site AS SELECT * FROM local.silver.dim_cell_site")

    query = """
    with dim_cell_site_source as (
        select 
            distinct cell_id as cell_id,
            city as city,
            site_name as site_name,
            latitude as latitude,
            longitude as longitude
        from dim_cell_site
    )
    select md5(cell_id || city ) as cell_sk , * from dim_cell_site_source
    """

    df = spark.sql(query)
    write_to_iceberg(df, "gold.dim_cell_site", mode="overwrite")
    logger.info("dim_cell_site loaded")