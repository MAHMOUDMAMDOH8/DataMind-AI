import os
import json
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

REQUESTS_TIMEOUT = 30

SKIP_DIRS = {"__pycache__", ".git", "scripts"}
SKIP_FILES = {"__init__.py", ".gitkeep", "test.py", "run.sh"}


class SparkJobClient:
    def __init__(self, jobs_path: str, master_url: Optional[str] = None):
        self.jobs_path = jobs_path
        self.master_url = master_url

    def discover_pipelines(self) -> List[dict]:
        pipelines = []
        if not os.path.isdir(self.jobs_path):
            logger.warning(f"Jobs path does not exist: {self.jobs_path}")
            return pipelines

        for entry in sorted(os.listdir(self.jobs_path)):
            if entry.startswith(".") or entry in SKIP_DIRS | SKIP_FILES:
                continue
            full_path = os.path.join(self.jobs_path, entry)

            if os.path.isfile(full_path) and entry.endswith(".py"):
                pipelines.append(self._make_pipeline(entry.replace(".py", ""), full_path, "file"))
            elif os.path.isdir(full_path):
                pipelines.extend(self._scan_directory(full_path, entry))

        return pipelines

    def _scan_directory(self, dir_path: str, rel_name: str) -> List[dict]:
        pipelines = []
        main_py = os.path.join(dir_path, "main.py")
        if os.path.isfile(main_py):
            tasks = self._discover_tasks(dir_path, main_py)
            pipelines.append(self._make_pipeline(rel_name, dir_path, "dir", tasks))
        else:
            for entry in sorted(os.listdir(dir_path)):
                if entry.startswith(".") or entry in SKIP_DIRS:
                    continue
                child = os.path.join(dir_path, entry)
                if os.path.isfile(child) and entry.endswith(".py"):
                    pipelines.append(self._make_pipeline(f"{rel_name}_{entry.replace('.py', '')}", child, "file"))
                elif os.path.isdir(child):
                    pipelines.extend(self._scan_directory(child, f"{rel_name}_{entry}"))
        return pipelines

    def _discover_tasks(self, dir_path: str, main_py: str) -> List[dict]:
        tasks = []
        for root, _, files in os.walk(dir_path):
            for f in sorted(files):
                if not f.endswith(".py") or f in SKIP_FILES or f == "main.py":
                    continue
                rel = os.path.relpath(os.path.join(root, f), dir_path)
                tasks.append({
                    "name": rel.replace(".py", "").replace(os.sep, "."),
                    "displayName": f.replace(".py", ""),
                    "filePath": os.path.join(root, f),
                })
        return tasks

    def _make_pipeline(self, name: str, path: str, type_: str, tasks: Optional[List[dict]] = None) -> dict:
        return {
            "name": name,
            "path": path,
            "type": type_,
            "tasks": tasks or [],
        }

    def get_master_status(self) -> Optional[dict]:
        if not self.master_url:
            return None
        try:
            res = requests.get(f"{self.master_url.rstrip('/')}/json/", timeout=REQUESTS_TIMEOUT)
            res.raise_for_status()
            return res.json()
        except Exception as err:
            logger.warning(f"Spark Master API error: {err}")
            return None

    def test_connection(self) -> bool:
        return os.path.isdir(self.jobs_path)
