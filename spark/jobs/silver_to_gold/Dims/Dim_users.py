# pyrefly: ignore [missing-import]
from scripts.spark_init import write_to_iceberg , get_spark_session
import logging

logger = logging.getLogger(__name__)

def load_dim_user(spark):
    
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_user AS SELECT * FROM local.silver.dim_user")

    query = """
    with dim_user_source as (
        select 
            distinct user_sk as user_id,
            msisdn as phone_number,
            customer_type as plan_type,
            gender,
            age_group as age_group,
            city as city,
            activation_date as activation_date,
            status as status,
            effective_from as effective_from,
            effective_to as effective_to,
            is_current as is_current
        from dim_user )
    select md5(user_id || phone_number ) as user_sk , * from dim_user_source
    """

    df = spark.sql(query)
    # wite to gold iceberg
    write_to_iceberg(df, "gold.dim_user", mode="overwrite")
    logger.info("dim_user loaded")

