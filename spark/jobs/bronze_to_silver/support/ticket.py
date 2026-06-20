import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.transformations import normalize_columns, add_rejection_reason
from scripts.spark_init import write_pipeline_metadata_event


def transform_ticket(df, metadata_endpoint: str = "http://minio:9000"):
    df = normalize_columns(df, "from", "from")
    df = normalize_columns(df, "to", "to")

    final_df = add_rejection_reason(
        df,
        required_columns=[
            "event_type", "sid", "customer", "phone_number",
            "channel", "reason", "priority", "status",
            "timestamp", "ticket_id",
        ],
    )

    raw_count = df.count()
    valid_count = final_df.filter("is_rejected = false").count()
    rejected_count = final_df.filter("is_rejected = true").count()

    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="support.tickets",
        action="transform",
        row_count=valid_count,
        target="local.silver.tickets",
        status="success",
        extra={
            "raw_count": raw_count,
            "rejected_count": rejected_count,
        },
    )

    return final_df
