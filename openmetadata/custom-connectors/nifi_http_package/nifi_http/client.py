import traceback
from typing import Any, Iterable, List, Optional

import requests

logger = None
try:
    from metadata.utils.logger import ingestion_logger
    logger = ingestion_logger()
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

REQUESTS_TIMEOUT = 60 * 5


class NifiHttpClient:
    def __init__(self, host_port: str):
        self.api_endpoint = host_port.rstrip("/") + "/nifi-api"

    def _get(self, path: str) -> Optional[Any]:
        try:
            res = requests.get(
                f"{self.api_endpoint}/{path}",
                timeout=REQUESTS_TIMEOUT,
            )
            res.raise_for_status()
            return res.json()
        except Exception as err:
            logger.warning(f"NiFi API error: {err}")
            raise

    def get_root_process_group_id(self) -> str:
        resources = self._get("resources")
        for r in (resources.get("resources") or []):
            identifier = r.get("identifier", "")
            if identifier.startswith("/process-groups/") and not identifier.startswith(
                "/process-groups/root"
            ):
                return identifier.replace("/process-groups/", "")
        return "root"

    def get_process_group_flow(self, pg_id: str) -> dict:
        return self._get(f"flow/process-groups/{pg_id}")

    def list_process_groups_recursive(
        self, pg_id: str = None, depth: int = 0
    ) -> Iterable[dict]:
        if pg_id is None:
            pg_id = self.get_root_process_group_id()
        flow = self.get_process_group_flow(pg_id)
        pg_flow = flow.get("processGroupFlow", {})
        breadcrumb = pg_flow.get("breadcrumb", {})
        pg_name = breadcrumb.get("breadcrumb", {}).get("name", pg_id)

        yield {
            "id": pg_id,
            "name": pg_name,
            "breadcrumb": breadcrumb,
            "flow": pg_flow.get("flow", {}),
        }

        for child_pg in pg_flow.get("flow", {}).get("processGroups", []):
            child_id = child_pg.get("component", {}).get("id")
            if child_id:
                yield from self.list_process_groups_recursive(child_id, depth + 1)

    def get_pg_processors(self, pg_id: str) -> List[dict]:
        flow = self.get_process_group_flow(pg_id)
        return (
            flow.get("processGroupFlow", {})
            .get("flow", {})
            .get("processors", [])
        )

    def test_connection(self) -> bool:
        resources = self._get("resources")
        return resources is not None
