import os
import yaml
import json
from pathlib import Path
from typing import Optional
from trino.dbapi import connect as trino_connect
from trino.exceptions import TrinoQueryError


DEFINITIONS_DIR = Path(__file__).parent / "definitions"

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER = os.getenv("TRINO_USER", "semantic")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gold")


class SemanticLayer:
    def __init__(self):
        self.metrics = {}
        self.dimensions = {}
        self.entities = {}
        self.relationships = []
        self.catalog = {}
        self._load()

    def _load(self):
        for name in ["metrics", "dimensions", "entities", "relationships", "catalog"]:
            path = DEFINITIONS_DIR / f"{name}.yaml"
            if path.exists():
                with open(path) as f:
                    data = yaml.safe_load(f)
                    if data:
                        setattr(self, name, data.get(name, data))
        print(f"  Semantic Layer loaded: {len(self.metrics)} metrics, "
              f"{len(self.dimensions)} dimensions, {len(self.entities)} entities, "
              f"{len(self.relationships)} relationships")

    def get_context(self) -> str:
        lines = ["You are a Trino SQL analyst for DataMind AI telecom platform.",
                 "Schema: iceberg.gold (Trino at localhost:8085)",
                 "",
                 "=== AVAILABLE TABLES ==="]
        for tname, tinfo in sorted(self.catalog.get("tables", {}).items()):
            cols = ", ".join(tinfo.get("columns", []))
            lines.append(f"- iceberg.gold.{tname} ({tinfo.get('label', tname)})")
            lines.append(f"  Description: {tinfo.get('description', '')}")
            lines.append(f"  Columns: {cols}")
            lines.append(f"  Grain: {tinfo.get('grain', 'N/A')}")
            lines.append("")

        lines.append("=== METRICS ===")
        for name, m in sorted(self.metrics.items()):
            lines.append(f"- {m['label']} ({name}): {m['description']}")
            lines.append(f"  SQL: {m['sql']}")
            lines.append(f"  Table: {m.get('table', 'N/A')}")
            lines.append(f"  Grain: {m.get('grain', 'N/A')}")
            if m.get("filters"):
                dc = m["filters"].get("date_column")
                if dc:
                    lines.append(f"  Date filter: {dc}")
            lines.append("")

        lines.append("=== DIMENSIONS ===")
        for name, d in sorted(self.dimensions.items()):
            lines.append(f"- {d['label']} ({name})")
            if d.get("fields"):
                for fn, fv in d["fields"].items():
                    desc = fv.get("description", "")
                    vals = fv.get("values")
                    if vals:
                        desc += f" [{', '.join(vals)}]"
                    lines.append(f"  - {fn}: {desc}")
            lines.append("")

        lines.append("=== RELATIONSHIPS ===")
        rels = self.relationships.values() if isinstance(self.relationships, dict) else self.relationships
        for r in rels:
            lines.append(f"- {r.get('label', 'N/A')}: {r.get('description', '')}")
            lines.append(f"  {r['from_entity']} -> {r['to_entity']} ({r['type']})")
            lines.append("")

        lines.append("=== RULES ===")
        lines.append("- Always use fully qualified names: iceberg.gold.<table>")
        lines.append("- Use the metrics, dimensions, and relationships defined above to construct SQL queries")
        lines.append("- Use Trino SQL syntax")
        lines.append("- When a date filter is needed, use the date_column hint from the metric definition")
        lines.append("- Join tables using the relationships defined above")
        lines.append("- Always use EXACT column names from the table schemas listed above")
        lines.append("- Check the AVAILABLE TABLES section above before generating SQL")
        lines.append("- Return results as a table")
        lines.append("- Add meaningful column aliases (AS alias_name) so results have readable column names")

        return "\n".join(lines)

    def get_metrics_list(self):
        return [
            {"name": k, "label": v["label"],
             "description": v["description"],
             "sql": v["sql"], "table": v.get("table"),
             "grain": v.get("grain"), "unit": v.get("unit"),
             "type": v.get("type")}
            for k, v in sorted(self.metrics.items())
        ]

    def get_dimensions_list(self):
        return [
            {"name": k, "label": v["label"], "description": v["description"]}
            for k, v in sorted(self.dimensions.items())
        ]

    def get_entities_list(self):
        return [
            {"name": k, "label": v["label"],
             "description": v["description"],
             "table": v.get("table"),
             "attributes": v.get("attributes", [])}
            for k, v in sorted(self.entities.items())
        ]

    def get_relationships_list(self):
        rels = self.relationships.values() if isinstance(self.relationships, dict) else self.relationships
        return [
            {"label": r.get("label"), "description": r.get("description"),
             "from": r["from_entity"], "to": r["to_entity"], "type": r["type"],
             "condition": r.get("join", {}).get("condition", "")}
            for r in rels
        ]

    def lookup_metric(self, name):
        return self.metrics.get(name)

    def execute_query(self, sql: str) -> dict:
        sql = sql.strip().rstrip(";")
        if not sql.upper().startswith("SELECT") and not sql.upper().startswith("WITH"):
            return {"error": "Only SELECT queries are permitted", "sql": sql}
        forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"]
        import re
        for keyword in forbidden:
            if re.search(rf"\b{keyword}\b", sql.upper()):
                return {"error": f"Query contains forbidden keyword: {keyword}", "sql": sql}
        try:
            conn = trino_connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user=TRINO_USER,
                catalog=TRINO_CATALOG,
                schema=TRINO_SCHEMA,
            )
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            cursor.close()
            conn.close()
            return {"columns": cols, "rows": rows, "row_count": len(rows)}
        except TrinoQueryError as e:
            return {"error": str(e), "sql": sql}
        except Exception as e:
            return {"error": f"Connection error: {e}", "sql": sql}


_layer = None


def get_semantic_layer():
    global _layer
    if _layer is None:
        _layer = SemanticLayer()
    return _layer
