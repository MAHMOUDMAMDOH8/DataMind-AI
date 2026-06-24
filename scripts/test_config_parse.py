import yaml, re
from metadata.ingestion.api.parser import parse_workflow_config_gracefully

content = open("/opt/airflow/dags/NiFi_Ingestion.py").read()
m = re.search(r"'''\n(.*?)'''", content, re.DOTALL)
if m:
    config = yaml.safe_load(m.group(1))
    print("Config loaded OK")
    print("source.type:", config.get("source", {}).get("type"))
    result = parse_workflow_config_gracefully(config)
    print("Parse OK")
