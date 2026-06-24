from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.entity.services.databaseService import DatabaseService

JWT = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3ODIzMDA3NjYsImV4cCI6bnVsbH0.N1xfx_7omX8lprcx-d3XVV1V7XcPkOCnzHt-pmSRGaRbYhyKXjNCDhKSw7cdn44BIotACGFIwzAkURnyMob-KblYcU5J562s1LgCr95bsCmJxAC6FBvM_8DhNPzNplKmuXUhFdHdCoNOhlTxdWGp9muVcD2y7NKlz8XWNmoq1m3QNsz6B7h3VAfVSxz5c9ZPPdv4UhQr-q7Xxfhzw0Uqs3EVTfORVFgEE-8Jo-EKb_tLt1tKrqjl3iMjdDdsE1P35x8tmzNYl3deSpnZEIHE1VQg-NRdfP4kENfTV2q0mPRf3Iktj4yzXmR04TNV7SETCoUpBeM5wBHF5Xs7XjWqpg"

cfg = OpenMetadataConnection(hostPort="http://openmetadata-server:8585/api", authProvider="openmetadata", securityConfig=OpenMetadataJWTClientConfig(jwtToken=JWT))
m = OpenMetadata(cfg)

print("=== Database Services ===")
dbs = m.list_services(entity=DatabaseService)
for db in dbs or []:
    print(f"  {db.name.root} -> FQN: {db.fullyQualifiedName.root}")

print("\n=== Databases ===")
dbs_list = m.list_entities(entity=Database).entities
for db in dbs_list:
    print(f"  {db.name.root} (service={db.service.name})")

print("\n=== Schemas ===")
schemas = m.list_entities(entity=DatabaseSchema).entities
for s in schemas[:30]:
    print(f"  {s.name.root} (database={s.database.name})")

print("\n=== Tables (first 30) ===")
tables = m.list_entities(entity=Table).entities
for t in tables[:30]:
    print(f"  {t.fullyQualifiedName.root}")

print("\n=== Topics ===")
topics = m.list_entities(entity=Topic).entities
for t in topics:
    print(f"  {t.fullyQualifiedName.root}")
