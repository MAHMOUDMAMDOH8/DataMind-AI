import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.transformations import normalize_columns, add_rejection_reason
from scripts.spark_init import write_pipeline_metadata_event


def transform_data_usage(df, metadata_endpoint: str = "http://minio:9000"):
    df = normalize_columns(df, "billing_info", "")

    final_df = add_rejection_reason(
        df,
        required_columns=[
            "event_type", "sid", "customer", "phone_number",
            "session_id", "data_used_mb", "duration_seconds",
            "session_type", "status", "timestamp", "amount", "currency",
        ],
        numeric_columns=["data_used_mb", "duration_seconds", "amount"],
        positive_columns=["data_used_mb", "duration_seconds"],
        is_between_columns={"data_used_mb": (0, 100000)},
    )

    raw_count = df.count()
    valid_count = final_df.filter("is_rejected = false").count()
    rejected_count = final_df.filter("is_rejected = true").count()

    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="data_usage.sessions",
        action="transform",
        row_count=valid_count,
        target="local.silver.data_usage",
        status="success",
        extra={
            "raw_count": raw_count,
            "rejected_count": rejected_count,
        },
    )

    return final_df
