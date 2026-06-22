import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import get_spark_session

from dim_date import load_dim_date
from dim_time import load_dim_time
from Dim_users import load_dim_user
from dim_device import load_dim_device
from dim_cell_site import load_dim_cell_site
from dim_agent import load_dim_agent

if __name__ == "__main__":
    spark = get_spark_session(app_name="load_all_dims")

    load_dim_date(spark)
    load_dim_time(spark)
    load_dim_user(spark)
    load_dim_device(spark)
    load_dim_cell_site(spark)
    load_dim_agent(spark)

    spark.stop()
