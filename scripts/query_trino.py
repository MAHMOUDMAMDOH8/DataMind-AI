import requests, json, time

def query_trino(sql):
    url = 'http://localhost:8085/v1/statement'
    resp = requests.post(url, data=sql, headers={'Content-Type': 'text/plain', 'X-Trino-User': 'admin'})
    data = resp.json()

    rows = []
    while True:
        cols = data.get('columns', [])
        if data.get('data'):
            rows.extend(data['data'])
        if data.get('nextUri'):
            resp = requests.get(data['nextUri'], headers={'X-Trino-User': 'admin'})
            data = resp.json()
        else:
            break

    return [c['name'] for c in cols], rows

cols, rows = query_trino("SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema = 'gold' ORDER BY table_name")
print(f'Gold tables ({len(rows)}):')
for r in rows:
    print(f'  - {r[0]}')

for tbl in [r[0] for r in rows]:
    cols2, rows2 = query_trino(f"SELECT count(*) as cnt FROM iceberg.gold.\"{tbl}\"")
    print(f'  {tbl}: {rows2[0][0]} rows')
