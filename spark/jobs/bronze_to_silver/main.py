import sys
sys.path.insert(0, "/home/iceberg/jobs")

from scripts.spark_init import (
    get_spark_session,
    read_bronze_table,
    write_to_silver,
    move_to_archive,
    delete_raws_in_bronze,
    write_pipeline_metadata_event,
)
from bronze_to_silver.billing.calls import transform_calls
from bronze_to_silver.billing.sms import transform_sms
from bronze_to_silver.data_usage.data_usage import transform_data_usage
from bronze_to_silver.crm.CRM import transform_crm
from bronze_to_silver.network.network import transform_network
from bronze_to_silver.payment_and_recharge.payment import transform_payment
from bronze_to_silver.payment_and_recharge.recharge import transform_recharge
from bronze_to_silver.roaming.roaming import transform_roaming
from bronze_to_silver.support.ticket import transform_ticket

ENDPOINT = "http://minio:9000"


def write_rejected(df, rejected_entity):
    rejected = df.filter("is_rejected = true")
    rejected_count = rejected.count()
    if rejected_count > 0:
        path = f"s3a://telecom-bronze/archive/rejected/{rejected_entity}/"
        rejected.write.mode("append").parquet(path)
        print(f"  Rejected {rejected_count} records -> archive/rejected/{rejected_entity}")


def process_entity(spark, raw, transform_fn, entity, silver_name, metadata_endpoint, bronze_base_layer="", bronze_table_name=None, split_names=None):
    bronze_table = bronze_table_name or entity
    result = transform_fn(raw, metadata_endpoint=metadata_endpoint)

    if isinstance(result, tuple):
        for i, vdf in enumerate(result):
            valid = vdf.filter("is_rejected = false")
            valid_count = valid.count()
            name = split_names[i] if split_names and i < len(split_names) else (f"{silver_name}_{i}" if i > 0 else silver_name)
            if valid_count > 0:
                write_to_silver(valid, name)
            write_rejected(vdf, name)

        move_to_archive(raw, entity, bronze_base_layer=bronze_base_layer)
        delete_raws_in_bronze(bronze_table, bronze_base_layer=bronze_base_layer)

        write_pipeline_metadata_event(
            metadata_endpoint,
            pipeline_stage="bronze_to_silver",
            entity=entity,
            action="archive_and_cleanup",
            target=silver_name,
            status="success",
        )
    else:
        valid = result.filter("is_rejected = false")
        valid_count = valid.count()
        if valid_count > 0:
            write_to_silver(valid, silver_name)

        write_rejected(result, silver_name)

        move_to_archive(valid, entity, bronze_base_layer=bronze_base_layer)
        delete_raws_in_bronze(bronze_table, bronze_base_layer=bronze_base_layer)

        write_pipeline_metadata_event(
            metadata_endpoint,
            pipeline_stage="bronze_to_silver",
            entity=entity,
            action="archive_and_cleanup",
            row_count=valid_count,
            target=silver_name,
            status="success",
        )


def run_all():
    spark = get_spark_session("BronzeToSilver")

    pipelines = [
        ("calls", "telecom-bronze", "", "calls", "billing.calls", transform_calls),
        ("sms", "telecom-bronze", "", "sms", "billing.sms", transform_sms),
        ("CRM", "telecom-bronze", "", "CRM", "crm.registration", transform_crm, ["crm_registration", "crm_profile_update"]),
        ("Network", "telecom-bronze", "", "Network", "network.events", transform_network, ["network_metrics", "network_qos_reports"]),
        ("Payments", "telecom-bronze", "", "Payments", "payment.payments", transform_payment),
        ("Recharge", "telecom-bronze", "", "Recharge", "recharge.recharges", transform_recharge),
        ("Roaming", "telecom-bronze", "", "Roaming", "roaming.sessions", transform_roaming),
        ("data_usage", "telecom-bronze", "", "DataUsage", "data_usage.sessions", transform_data_usage),
        ("Support", "telecom-bronze", "", "Support", "support.tickets", transform_ticket),
    ]

    for pipeline_entry in pipelines:
        table_name, bucket, base_layer, silver_name, entity, transform_fn = pipeline_entry[:6]
        split_names = pipeline_entry[6] if len(pipeline_entry) > 6 else None
        print(f"\n=== {entity} ===")
        try:
            try:
                raw = read_bronze_table(spark, table_name=table_name, bucket=bucket, base_layer=base_layer)
                raw_count = raw.count()
            except Exception as e:
                err_msg = str(e)
                if "PATH_NOT_FOUND" in err_msg or "does not exist" in err_msg.lower() or "not found" in err_msg.lower():
                    print(f"No data in bronze for {table_name}, skipping")
                    write_pipeline_metadata_event(
                        ENDPOINT,
                        pipeline_stage="bronze_to_silver",
                        entity=entity,
                        action="skip_empty",
                        row_count=0,
                        target=silver_name,
                        status="skipped",
                        extra={"raw_count": 0, "error": f"bronze path not found: {err_msg}"},
                    )
                    continue
                raise

            if raw_count == 0:
                print("No data, skipping")
                write_pipeline_metadata_event(
                    ENDPOINT,
                    pipeline_stage="bronze_to_silver",
                    entity=entity,
                    action="skip_empty",
                    row_count=0,
                    target=silver_name,
                    status="skipped",
                    extra={"raw_count": 0},
                )
                continue

            print(f"Read {raw_count} records")
            process_entity(spark, raw, transform_fn, entity, silver_name, ENDPOINT, bronze_table_name=table_name, split_names=split_names)

        except Exception as e:
            print(f"ERROR: {e}")
            write_pipeline_metadata_event(
                ENDPOINT,
                pipeline_stage="bronze_to_silver",
                entity=entity,
                action="failed",
                status="failed",
                error_message=str(e),
            )

    spark.stop()
    print("\n=== Done ===")


if __name__ == "__main__":
    run_all()
