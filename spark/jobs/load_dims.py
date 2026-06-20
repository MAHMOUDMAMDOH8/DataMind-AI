import sys
sys.path.insert(0, "/home/iceberg/jobs")

from datetime import date

from pyspark.sql.functions import col, lit, max as spark_max, md5, concat_ws, coalesce, to_date, row_number
from pyspark.sql.window import Window

from scripts.spark_init import (
    get_spark_session,
    read_from_iceberg,
    write_pipeline_metadata_event,
)

ENDPOINT = "http://minio:9000"
BRONZE_BUCKET = "s3a://telecom-bronze"
SILVER_BUCKET = "s3a://telecom-silver"

DIM_CONFIGS = [
    {
        "name": "dim_user",
        "file": "DIM_USER.json",
        "natural_key": "msisdn",
        "sk_col": "user_sk",
        "has_scd2": True,
        "business_attrs": ["msisdn", "customer_type", "gender", "age_group", "city", "activation_date", "status"],
    },
    {
        "name": "dim_device",
        "file": "DIM_DEVICE.json",
        "natural_key": "imei",
        "sk_col": "device_sk",
        "has_scd2": False,
        "business_attrs": ["imei", "tac", "brand", "model", "os", "is_smartphone"],
    },
    {
        "name": "dim_agent",
        "file": "dim_agent.json",
        "natural_key": "agent_id",
        "sk_col": "agent_sk",
        "has_scd2": False,
        "business_attrs": ["agent_id", "name", "department", "city", "status"],
    },
    {
        "name": "dim_cell_site",
        "file": "dim_cell_site.json",
        "natural_key": "cell_id",
        "sk_col": "cell_sk",
        "has_scd2": False,
        "business_attrs": ["cell_id", "city", "site_name", "latitude", "longitude"],
    },
]


def read_bronze_dim(spark, file_name):
    return spark.read.option("multiLine", "true").json(f"{BRONZE_BUCKET}/DIMs/{file_name}")


def add_hash(df, attrs):
    cols = [coalesce(col(c).cast("string"), lit("")) for c in attrs]
    return df.withColumn("_hash", md5(concat_ws("|", *cols)))


def assign_sk(df, sk_col, nk, start_from):
    w = Window.orderBy(nk)
    return df.withColumn(sk_col, (row_number().over(w) + lit(start_from)).cast("int"))


def scd2_cols(df, sk_col, nk, today, start_sk=0):
    df = assign_sk(df, sk_col, nk, start_sk)
    df = df.withColumn("effective_from", to_date(lit(today.isoformat())))
    df = df.withColumn("effective_to", to_date(lit("9999-12-31")))
    df = df.withColumn("is_current", lit(True))
    return df


def write_iceberg(df, table_name):
    spark = df.sparkSession
    full = f"local.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {full}")
    df.createOrReplaceTempView("_dim_iceberg_tmp")
    spark.sql(f"CREATE TABLE {full} USING iceberg AS SELECT * FROM _dim_iceberg_tmp")


def load_dim(spark, cfg):
    name = cfg["name"]
    nk = cfg["natural_key"]
    sk_col = cfg["sk_col"]
    attrs = cfg["business_attrs"]
    today = date.today()
    print(f"\n=== {name} ===")

    raw = read_bronze_dim(spark, cfg["file"])
    raw_count = raw.count()
    print(f"  Read {raw_count} rows from bronze")

    existing = read_from_iceberg(name, spark)

    if existing is None:
        df = raw
        if not cfg["has_scd2"]:
            df = scd2_cols(df, sk_col, nk, today)
        write_iceberg(df, name)
        df.write.mode("overwrite").parquet(f"{SILVER_BUCKET}/{name}/")
        write_pipeline_metadata_event(ENDPOINT, pipeline_stage="dim_load", entity=name,
                                      action="initial_load", row_count=raw_count, target=name, status="success")
        print(f"  Loaded {raw_count} rows")
        return

    raw_hashed = add_hash(raw, attrs)
    current = existing.filter(col("is_current") == True)
    current_hashed = add_hash(current, attrs)

    new_nk = raw_hashed.join(current_hashed.select(nk), nk, "left_anti").select(nk)
    changed_nk = raw_hashed.alias("r").join(current_hashed.alias("c"), nk, "inner") \
        .filter(col("r._hash") != col("c._hash")).select(col(nk))
    unchanged_nk = raw_hashed.alias("r").join(current_hashed.alias("c"), nk, "inner") \
        .filter(col("r._hash") == col("c._hash")).select(col(nk))

    new_count = new_nk.count()
    changed_count = changed_nk.count()
    unchanged_count = unchanged_nk.count()
    print(f"  New: {new_count}, Changed: {changed_count}, Unchanged: {unchanged_count}")

    if new_count == 0 and changed_count == 0:
        print("  No changes")
        return

    parts = []
    max_sk = existing.agg(spark_max(col(sk_col).cast("int")).alias("mx")).collect()[0]["mx"] or 0

    unchanged = current.join(unchanged_nk, nk)
    parts.append(unchanged)

    if changed_count > 0:
        expired = current.join(changed_nk, nk) \
            .withColumn("effective_to", to_date(lit(today.isoformat()))) \
            .withColumn("is_current", lit(False))
        parts.append(expired)

        new_vers = raw_hashed.join(changed_nk, nk).drop("_hash")
        new_vers = assign_sk(new_vers, sk_col, nk, max_sk)
        new_vers = new_vers.withColumn("effective_from", to_date(lit(today.isoformat())))
        new_vers = new_vers.withColumn("effective_to", to_date(lit("9999-12-31")))
        new_vers = new_vers.withColumn("is_current", lit(True))
        parts.append(new_vers)

    if new_count > 0:
        ins = raw_hashed.join(new_nk, nk).drop("_hash")
        ins = assign_sk(ins, sk_col, nk, max_sk + changed_count)
        ins = ins.withColumn("effective_from", to_date(lit(today.isoformat())))
        ins = ins.withColumn("effective_to", to_date(lit("9999-12-31")))
        ins = ins.withColumn("is_current", lit(True))
        parts.append(ins)

    final = parts[0]
    for p in parts[1:]:
        final = final.unionByName(p, allowMissingColumns=True)

    write_iceberg(final, name)
    final.write.mode("overwrite").parquet(f"{SILVER_BUCKET}/{name}/")

    write_pipeline_metadata_event(ENDPOINT, pipeline_stage="dim_load", entity=name,
                                  action="incremental_load", row_count=new_count + changed_count,
                                  target=name, status="success")
    print(f"  Wrote {final.count()} rows to {name}")


def run():
    spark = get_spark_session("LoadDIMs")

    for cfg in DIM_CONFIGS:
        try:
            load_dim(spark, cfg)
        except Exception as e:
            print(f"ERROR loading {cfg['name']}: {e}")
            import traceback
            traceback.print_exc()
            write_pipeline_metadata_event(ENDPOINT, pipeline_stage="dim_load",
                                          entity=cfg["name"], action="failed",
                                          status="failed", error_message=str(e))

    spark.stop()
    print("\n=== DIM load complete ===")


if __name__ == "__main__":
    run()
