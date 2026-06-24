from nifi_http.client import NifiHttpClient

c = NifiHttpClient('http://nifi:8080')
print('Root PG ID:', c.get_root_process_group_id())
pgs = list(c.list_process_groups_recursive())
print('Process groups found:', len(pgs))
for pg in pgs:
    name = pg.get('name', '?')
    pg_id = pg.get('id', '?')
    print('  -', name, '(' + pg_id + ')')
    procs = pg['flow'].get('processors', [])
    print('      Processors:', len(procs))
