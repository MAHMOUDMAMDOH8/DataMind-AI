import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.transformations import normalize_columns, add_rejection_reason
from scripts.spark_init import write_pipeline_metadata_event


def transform_payment(df, metadata_endpoint: str = "http://minio:9000"):
    df = normalize_columns(df, "billing_info", "")

    final_df = add_rejection_reason(
        df,
        required_columns=[
            "event_type", "sid", "customer", "payment_amount",
            "payment_method", "status", "timestamp", "phone_number",
            "transaction_id", "amount", "currency",
        ],
        numeric_columns=["payment_amount", "amount"],
        positive_columns=["payment_amount"],
    )

    raw_count = df.count()
    valid_count = final_df.filter("is_rejected = false").count()
    rejected_count = final_df.filter("is_rejected = true").count()

    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="payment.payments",
        action="transform",
        row_count=valid_count,
        target="local.silver.payments",
        status="success",
        extra={
            "raw_count": raw_count,
            "rejected_count": rejected_count,
        },
    )

    return final_df
