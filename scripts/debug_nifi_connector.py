from nifi_http.client import NifiHttpClient
from nifi_http.metadata import NifiHttpPipelineDetails, _get_processors_from_process_group, _get_connections_from_process_group

c = NifiHttpClient("http://nifi:8080")

print("Testing list_process_groups_recursive:")
count = 0
for pg in c.list_process_groups_recursive():
    count += 1
    name = pg.get("name", "?")
    pg_id = pg.get("id", "?")
    procs = _get_processors_from_process_group(pg)
    conns = _get_connections_from_process_group(pg)
    print(f"  {name} ({pg_id}): {len(procs)} processors, {len(conns)} connections")
    if count >= 3:
        break

print(f"\nTotal process groups: {count}")
print("\nAll PGs:")
for pg in c.list_process_groups_recursive():
    name = pg.get("name", "?")
    pg_id = pg.get("id", "?")
    procs = _get_processors_from_process_group(pg)
    conns = _get_connections_from_process_group(pg)
    print(f"  {name}: {len(procs)}p/{len(conns)}c")
