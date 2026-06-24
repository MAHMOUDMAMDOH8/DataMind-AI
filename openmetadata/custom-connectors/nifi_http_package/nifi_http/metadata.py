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
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import ingestion_logger

from nifi_http.client import NifiHttpClient

logger = ingestion_logger()

PROCESS_GROUP_FLOW = "processGroupFlow"
BREADCRUMB = "breadcrumb"


class NifiHttpProcessor(BaseModel):
    id_: str
    name: Optional[str] = None
    type_: str
    uri: str


class NifiHttpProcessorConnections(BaseModel):
    id_: str
    source_id: str
    destination_id: str


class NifiHttpPipelineDetails(BaseModel):
    id_: str
    name: Optional[str] = None
    uri: str
    processors: List[NifiHttpProcessor]
    connections: List[NifiHttpProcessorConnections]


class NifiHttpSource(PipelineServiceSource):
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

    @staticmethod
    def _get_downstream_tasks_from(
        source_id: str, connections: List[NifiHttpProcessorConnections]
    ) -> List[str]:
        return [
            conn.destination_id for conn in connections if conn.source_id == source_id
        ]

    def _get_tasks_from_details(
        self, pipeline_details: NifiHttpPipelineDetails
    ) -> Optional[List[Task]]:
        try:
            host_port = self.service_connection.connectionOptions.root.get(
                "hostPort", "http://localhost:8082"
            )
            return [
                Task(
                    name=processor.id_,
                    displayName=processor.name,
                    sourceUrl=SourceUrl(
                        f"{clean_uri(host_port)}{processor.uri}"
                    ),
                    taskType=processor.type_,
                    downstreamTasks=self._get_downstream_tasks_from(
                        source_id=processor.id_,
                        connections=pipeline_details.connections,
                    ),
                )
                for processor in pipeline_details.processors
            ]
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Error getting tasks from Pipeline Details {pipeline_details} - {err}."
            )
        return None

    def yield_pipeline(
        self, pipeline_details: NifiHttpPipelineDetails
    ) -> Iterable[Either[CreatePipelineRequest]]:
        host_port = self.service_connection.connectionOptions.root.get(
            "hostPort", "http://localhost:8082"
        )
        pipeline_request = CreatePipelineRequest(
            name=EntityName(pipeline_details.id_),
            displayName=pipeline_details.name,
            sourceUrl=SourceUrl(
                f"{clean_uri(host_port)}{pipeline_details.uri}"
            ),
            tasks=self._get_tasks_from_details(pipeline_details),
            service=FullyQualifiedEntityName(self.context.get().pipeline_service),
        )
        yield Either(right=pipeline_request)
        self.register_record(pipeline_request=pipeline_request)

    def yield_pipeline_status(
        self, pipeline_details: NifiHttpPipelineDetails
    ) -> Iterable[Either[OMetaPipelineStatus]]:
        yield from []

    def yield_pipeline_lineage_details(
        self, pipeline_details: NifiHttpPipelineDetails
    ) -> Iterable[Either[AddLineageRequest]]:
        yield from []

    @staticmethod
    def _get_connections_from_process_group(
        process_group: dict,
    ) -> List[NifiHttpProcessorConnections]:
        flow_data = process_group.get(PROCESS_GROUP_FLOW) or process_group
        flow = flow_data.get("flow") or flow_data
        connections_list = flow.get("connections") or []
        return [
            NifiHttpProcessorConnections(
                id_=connection.get("id"),
                source_id=connection["component"]["source"]["id"],
                destination_id=connection["component"]["destination"]["id"],
            )
            for connection in connections_list
        ]

    @staticmethod
    def _get_processors_from_process_group(process_group: dict) -> List[NifiHttpProcessor]:
        flow_data = process_group.get(PROCESS_GROUP_FLOW) or process_group
        flow = flow_data.get("flow") or flow_data
        processor_list = flow.get("processors") or []
        return [
            NifiHttpProcessor(
                id_=processor.get("id"),
                uri=processor.get("uri"),
                name=processor["component"].get("name"),
                type_=processor["component"].get("type"),
            )
            for processor in processor_list
        ]

    def get_pipelines_list(self) -> Iterable[NifiHttpPipelineDetails]:
        for process_group in self.connection.list_process_groups_recursive():
            try:
                pg_id = process_group.get("id")
                if not pg_id:
                    continue
                yield NifiHttpPipelineDetails(
                    id_=pg_id,
                    name=process_group.get("name"),
                    uri=f"/nifi-api/flow/process-groups/{pg_id}",
                    processors=self._get_processors_from_process_group(
                        process_group=process_group
                    ),
                    connections=self._get_connections_from_process_group(
                        process_group=process_group
                    ),
                )
            except (ValueError, KeyError, ValidationError) as err:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Cannot create NifiHttpPipelineDetails from {process_group} - {err}"
                )
            except Exception as err:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Error getting pipelines from Process Group {process_group} - {err}."
                )

    def get_pipeline_name(self, pipeline_details: NifiHttpPipelineDetails) -> str:
        return pipeline_details.name


def get_connection(connection: CustomPipelineConnection) -> NifiHttpClient:
    host_port = connection.connectionOptions.root.get(
        "hostPort", "http://localhost:8082"
    )
    return NifiHttpClient(host_port=host_port)


def test_connection(
    metadata: OpenMetadata,
    client: NifiHttpClient,
    service_connection: CustomPipelineConnection,
    automation_workflow=None,
) -> None:
    try:
        from metadata.ingestion.connections.test_connections import test_connection_steps

        def custom_executor():
            client.get_root_process_group_id()

        test_fn = {"GetPipelines": custom_executor}
        test_connection_steps(
            metadata=metadata,
            test_fn=test_fn,
            service_type=service_connection.type.value,
            automation_workflow=automation_workflow,
        )
    except Exception:
        client.get_root_process_group_id()
