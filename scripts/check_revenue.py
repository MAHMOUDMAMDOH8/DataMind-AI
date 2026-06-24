import requests, json, time

def query_trino(sql):
    url = 'http://localhost:8085/v1/statement'
    resp = requests.post(url, data=sql, headers={'Content-Type': 'text/plain', 'X-Trino-User': 'admin'})
    data = resp.json()
    rows = []
    while True:
        if data.get('data'):
            rows.extend(data['data'])
        if data.get('nextUri'):
            resp = requests.get(data['nextUri'], headers={'X-Trino-User': 'admin'})
            data = resp.json()
        else:
            break
    return [c['name'] for c in data.get('columns', [])], rows

cols, rows = query_trino("SELECT * FROM iceberg.gold.daily_revenue LIMIT 5")
print('daily_revenue:')
print(cols)
for r in rows:
    print(r)

cols2, rows2 = query_trino("SELECT * FROM iceberg.gold.dim_date LIMIT 5")
print('\ndim_date:')
print(cols2)
for r2 in rows2:
    print(r2)
