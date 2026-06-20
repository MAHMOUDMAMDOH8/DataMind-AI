import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.transformations import (
    normalize_columns,
    add_rejection_reason,
)
from scripts.spark_init import write_pipeline_metadata_event


def transform_crm(df, metadata_endpoint: str = "http://minio:9000"):



    registration_events = df.filter(
        "event_type = 'customer_registration'"
    )

    registration_events = add_rejection_reason(
        df=registration_events,
        required_columns=[
            "sid",
            "customer",
            "phone_number",
            "plan_type",
            "behavior_profile",
            "city",
            "region",
            "registration_date",
            "timestamp",
            "status",
            "channel",
            "seasonal_multiplier",
        ],
        numeric_columns=[
            "seasonal_multiplier",
        ],
        positive_columns=[
            "seasonal_multiplier",
        ],
        is_between_columns={
            "seasonal_multiplier": (0.0, 2.0),
        },
    )



    profile_update_events = df.filter(
        "event_type = 'customer_profile_update'"
    )
    profile_update_events = normalize_columns(profile_update_events, "updated_fields", "")

    profile_update_events = add_rejection_reason(
        df=profile_update_events,
        required_columns=[
            "sid",
            "customer",
            "phone_number",
            "updated_fields",
            "timestamp",
            "seasonal_multiplier",
        ],
        numeric_columns=[
            "seasonal_multiplier",
        ],
        positive_columns=[
            "seasonal_multiplier",
        ],
        is_between_columns={
            "seasonal_multiplier": (0.0, 2.0),
        },
    )
    # Write metadata events for registration events
    raw_registration_count = registration_events.count()
    valid_registration_count = registration_events.filter("is_rejected = false").count()
    rejected_registration_count = registration_events.filter("is_rejected = true").count()
    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="crm.registration",
        action="transform",
        row_count=valid_registration_count,
        target="local.silver.crm_registration",
        status="success",
        extra={
            "raw_count": raw_registration_count,
            "rejected_count": rejected_registration_count,
        },
    )
    # Write metadata events for profile update events
    raw_profile_update_count = profile_update_events.count()
    valid_profile_update_count = profile_update_events.filter("is_rejected = false").count()
    rejected_profile_update_count = profile_update_events.filter("is_rejected = true").count()
    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="crm.profile_update",
        action="transform",
        row_count=valid_profile_update_count,
        target="local.silver.crm_profile_update",
        status="success",
        extra={
            "raw_count": raw_profile_update_count,
            "rejected_count": rejected_profile_update_count,
        },
    )

    return registration_events, profile_update_events