import traceback
from typing import Iterable, List, Optional

from pydantic import BaseModel, ValidationError

from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.pipeline import Task
from metadata.generated.schema.entity.services.connections.pipeline.customPipelineConnection import (
    CustomPipelineConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    SourceUrl,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.pipeline_status import OMetaPipelineStatus
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.pipeline_service import PipelineServiceSource
from metadata.utils.logger import ingestion_logger

from spark_http.client import SparkJobClient

logger = ingestion_logger()


class SparkJobPipelineDetails(BaseModel):
    id_: str
    name: Optional[str] = None
    path: str
    tasks: List[dict]


class SparkJobSource(PipelineServiceSource):
    @classmethod
    def create(
        cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: CustomPipelineConnection = config.serviceConnection.root.config
        if not isinstance(connection, CustomPipelineConnection):
            raise InvalidSourceException(
                f"Expected CustomPipelineConnection, but got {connection}"
            )
        return cls(config, metadata)

    def _get_tasks_from_details(
        self, pipeline_details: SparkJobPipelineDetails
    ) -> Optional[List[Task]]:
        try:
            return [
                Task(
                    name=task["name"],
                    displayName=task.get("displayName"),
                    sourceUrl=SourceUrl(
                        f"file://{task['filePath']}"
                    ),
                    taskType="spark-job",
                )
                for task in pipeline_details.tasks
            ]
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Error getting tasks from Pipeline Details {pipeline_details} - {err}."
            )
        return None

    def yield_pipeline(
        self, pipeline_details: SparkJobPipelineDetails
    ) -> Iterable[Either[CreatePipelineRequest]]:
        pipeline_request = CreatePipelineRequest(
            name=EntityName(pipeline_details.id_),
            displayName=pipeline_details.name,
            sourceUrl=SourceUrl(
                f"file://{pipeline_details.path}"
            ),
            tasks=self._get_tasks_from_details(pipeline_details),
            service=FullyQualifiedEntityName(self.context.get().pipeline_service),
        )
        yield Either(right=pipeline_request)
        self.register_record(pipeline_request=pipeline_request)

    def yield_pipeline_status(
        self, pipeline_details: SparkJobPipelineDetails
    ) -> Iterable[Either[OMetaPipelineStatus]]:
        yield from []

    def yield_pipeline_lineage_details(
        self, pipeline_details: SparkJobPipelineDetails
    ) -> Iterable[Either[AddLineageRequest]]:
        yield from []

    def get_pipelines_list(self) -> Iterable[SparkJobPipelineDetails]:
        for pipeline in self.connection.discover_pipelines():
            try:
                yield SparkJobPipelineDetails(
                    id_=pipeline["name"],
                    name=pipeline["name"],
                    path=pipeline["path"],
                    tasks=pipeline.get("tasks", []),
                )
            except (ValueError, KeyError, ValidationError) as err:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Cannot create SparkJobPipelineDetails from {pipeline} - {err}"
                )
            except Exception as err:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Error getting pipeline from {pipeline} - {err}."
                )

    def get_pipeline_name(self, pipeline_details: SparkJobPipelineDetails) -> str:
        return pipeline_details.name


def get_connection(connection: CustomPipelineConnection) -> SparkJobClient:
    jobs_path = connection.connectionOptions.root.get(
        "jobsPath", "/home/iceberg/jobs"
    )
    master_url = connection.connectionOptions.root.get(
        "masterUrl", "http://spark-iceberg:8080"
    )
    return SparkJobClient(jobs_path=jobs_path, master_url=master_url)


def test_connection(
    metadata: OpenMetadata,
    client: SparkJobClient,
    service_connection: CustomPipelineConnection,
    automation_workflow=None,
) -> None:
    try:
        from metadata.ingestion.connections.test_connections import test_connection_steps

        def custom_executor():
            client.test_connection()

        test_fn = {"GetPipelines": custom_executor}
        test_connection_steps(
            metadata=metadata,
            test_fn=test_fn,
            service_type=service_connection.type.value,
            automation_workflow=automation_workflow,
        )
    except Exception:
        client.test_connection()
