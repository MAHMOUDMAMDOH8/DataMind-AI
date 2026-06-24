import json, requests
d = requests.get('http://localhost:8082/nifi-api/flow/process-groups/fa5e9f8b-019e-1000-d962-f6acf3351e1b').json()
pgs = d['processGroupFlow']['flow']['processGroups']
print(f'Process groups under root: {len(pgs)}')
for g in pgs:
    c = g['component']
    print(f'  - {c["name"]} (id={c["id"]})')
