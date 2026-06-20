import sys; sys.path.insert(0, "/home/iceberg/jobs")
from scripts.spark_init import get_spark_session

spark = get_spark_session("MigrateNamespace")

tables = [
    "customer_360", "customer_usage_daily", "daily_revenue",
    "fraud_monitoring", "network_performance", "payment_analytics",
    "recharge_analytics", "roaming_analytics", "support_analytics",
    "dim_date", "dim_time", "dim_user", "dim_device", "dim_agent", "dim_cell_site",
    "dim_user_gold", "dim_device_gold", "dim_agent_gold", "dim_cell_site_gold",
]

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
print("Namespace local.gold created")

for t in tables:
    try:
        cnt = spark.table(f"local.{t}").count()
        spark.sql(f"DROP TABLE IF EXISTS local.gold.{t}")
        spark.sql(f"CREATE TABLE local.gold.{t} USING iceberg AS SELECT * FROM local.{t}")
        print(f"  {t}: {cnt} rows migrated")
    except Exception:
        print(f"  {t}: SKIPPED (not found)")

spark.stop()
print("=== Done ===")
