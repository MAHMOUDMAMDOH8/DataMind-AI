import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.transformations import (
    normalize_columns,
    add_rejection_reason,
)
from scripts.spark_init import write_pipeline_metadata_event


def transform_network(df, metadata_endpoint: str = "http://minio:9000"):



    network_metric_events = df.filter(
        "event_type = 'network_metric'"
    )
    network_metric_events = normalize_columns(network_metric_events, "metrics", "")  
    network_metric_events = add_rejection_reason(
        df=network_metric_events,
        required_columns=[
            "sid",
            "cell_site_id",
            "city",
            "region",
            "network_type",
            "timestamp",
            "metrics",
            "active_subscribers",
            "total_throughput_mbps",
            "cpu_utilization_pct",
            "memory_utilization_pct",
        ],
        numeric_columns=[
            "active_subscribers",
            "total_throughput_mbps",
            "cpu_utilization_pct",
            "memory_utilization_pct",
        ],
        positive_columns=[
            "active_subscribers",
            "total_throughput_mbps",
        ],
        is_between_columns={
            "cpu_utilization_pct": (0, 100),
            "memory_utilization_pct": (0, 100),
        },
    )



    qos_report_events = df.filter(
        "event_type = 'qos_report'"
    )

    qos_report_events = add_rejection_reason(
        df=qos_report_events,
        required_columns=[
            "sid",
            "cell_site_id",
            "city",
            "region",
            "network_type",
            "timestamp",
            "mos_score_avg",
            "jitter_ms_avg",
            "packet_loss_pct_avg",
            "latency_ms_avg",
            "throughput_mbps_avg",
            "sample_size",
        ],
        numeric_columns=[
            "mos_score_avg",
            "jitter_ms_avg",
            "packet_loss_pct_avg",
            "latency_ms_avg",
            "throughput_mbps_avg",
            "sample_size",
        ],
        positive_columns=[
            "latency_ms_avg",
            "throughput_mbps_avg",
            "sample_size",
        ],
        is_between_columns={
            "mos_score_avg": (1.0, 5.0),
            "packet_loss_pct_avg": (0, 100),
        },
    )

    # Write metadata events for network metric events
    raw_network_count = network_metric_events.count()
    valid_network_count = network_metric_events.filter("is_rejected = false").count()
    rejected_network_count = network_metric_events.filter("is_rejected = true").count()

    # Write metadata events for QoS report events
    raw_qos_count = qos_report_events.count()
    valid_qos_count = qos_report_events.filter("is_rejected = false").count()
    rejected_qos_count = qos_report_events.filter("is_rejected = true").count()
    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="network.network_metrics",
        action="transform",
        row_count=valid_network_count,
        target="local.silver.network_metrics",
        status="success",
        extra={
            "raw_count": raw_network_count,
            "rejected_count": rejected_network_count,
        },
    )
    write_pipeline_metadata_event(
        metadata_endpoint,
        pipeline_stage="bronze_to_silver",
        entity="network.qos_reports",
        action="transform",
        row_count=valid_qos_count,
        target="local.silver.qos_reports",
        status="success",
        extra={
            "raw_count": raw_qos_count,
            "rejected_count": rejected_qos_count,
        },
    )
    return network_metric_events, qos_report_events