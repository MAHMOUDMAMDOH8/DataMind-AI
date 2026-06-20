from pyspark.sql.functions import expr

from scripts.transformations import normalize_columns, add_rejection_reason
from scripts.spark_init import write_pipeline_metadata_event

def transform_sms(df, metadata_endpoint: str = "http://minio:9000"):
    df = normalize_columns(df, "from", "from")
    df = normalize_columns(df, "to", "to")
    df = normalize_columns(df, "billing_info", "")
    df = normalize_columns(df, "qos_metrics", "")

    df = df.withColumn("from_tac", expr("substr(from_imei, 1, 8)"))
    df = df.withColumn("to_tac", expr("substr(to_imei, 1, 8)"))

    final_df = add_rejection_reason(
        df,
        required_columns=[
            "event_type", "sid", "timestamp", "status",
            "from_phone_number", "to_phone_number",
            "amount", "currency", 
        ],
        numeric_columns=["amount"],
        positive_columns=["amount"],
    )

    raw_count = df.count()
    valid_count = final_df.filter("is_rejected = false").count()
    rejected_count = final_df.filter("is_rejected = true").count()

    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="billing.sms",
        action="transform",
        row_count=valid_count,
        target="local.silver.sms",
        status="success",
        extra={
            "raw_count": raw_count,
            "rejected_count": rejected_count,
        },
    )

    return final_df