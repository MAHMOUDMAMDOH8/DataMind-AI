"""
Register the Spark CustomPipeline service in OpenMetadata.
Run inside the OM ingestion container.
"""
import json

from metadata.generated.schema.api.services.createPipelineService import (
    CreatePipelineServiceRequest,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.pipeline.customPipelineConnection import (
    CustomPipelineConnection,
    CustomPipelineType,
)
from metadata.generated.schema.entity.services.connections.connectionBasicType import (
    ConnectionOptions,
)
from metadata.generated.schema.entity.services.pipelineService import (
    PipelineService,
    PipelineConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.basic import EntityName
from metadata.ingestion.ometa.ometa_api import OpenMetadata


def main():
    JWT_TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3ODIzMDA3NjYsImV4cCI6bnVsbH0.N1xfx_7omX8lprcx-d3XVV1V7XcPkOCnzHt-pmSRGaRbYhyKXjNCDhKSw7cdn44BIotACGFIwzAkURnyMob-KblYcU5J562s1LgCr95bsCmJxAC6FBvM_8DhNPzNplKmuXUhFdHdCoNOhlTxdWGp9muVcD2y7NKlz8XWNmoq1m3QNsz6B7h3VAfVSxz5c9ZPPdv4UhQr-q7Xxfhzw0Uqs3EVTfORVFgEE-8Jo-EKb_tLt1tKrqjl3iMjdDdsE1P35x8tmzNYl3deSpnZEIHE1VQg-NRdfP4kENfTV2q0mPRf3Iktj4yzXmR04TNV7SETCoUpBeM5wBHF5Xs7XjWqpg"

    server_config = OpenMetadataConnection(
        hostPort="http://openmetadata-server:8585/api",
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=JWT_TOKEN),
    )
    m = OpenMetadata(server_config)
    print("Health:", m.health_check())

    # Check existing pipeline services
    services = m.list_services(entity=PipelineService)
    print("Existing pipeline services:")
    for s in services or []:
        ctype = s.connection.config.type.value
        print(f"  - {s.name.root} (type={ctype})")
        if ctype == "CustomPipeline":
            opts = s.connection.config.connectionOptions
            if opts:
                print(f"      sourcePythonClass: {opts.root.get('sourcePythonClass', 'N/A')}")

    # Create Spark pipeline service
    spark_name = "datamind-spark"
    # Check if already exists
    try:
        existing = m.get_by_name(entity=PipelineService, fqn=spark_name)
        if existing:
            print(f"\nService '{spark_name}' already exists, skipping creation.")
            return
    except Exception:
        pass

    custom_conn = CustomPipelineConnection(
        type=CustomPipelineType.CustomPipeline,
        sourcePythonClass="spark_http.metadata.SparkJobSource",
        connectionOptions=ConnectionOptions(
            root={
                "jobsPath": "/home/iceberg/jobs",
                "masterUrl": "http://spark-iceberg:8080",
            }
        ),
    )
    request = CreatePipelineServiceRequest(
        name=EntityName(spark_name),
        displayName="Spark Jobs",
        description="Custom pipeline service for discovering Spark batch job scripts from the filesystem",
        serviceType="CustomPipeline",
        connection=PipelineConnection(config=custom_conn),
    )
    created = m.create_or_update(request)
    print(f"\nCreated Spark pipeline service: {created.name.root} (id={created.id.root})")


if __name__ == "__main__":
    main()
