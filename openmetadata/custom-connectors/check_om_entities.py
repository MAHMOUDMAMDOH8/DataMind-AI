"""
Check OM entities for lineage mapping.
"""
import os
# load env
from dotenv import load_dotenv
load_dotenv()
from metadata.generated.schema.api.services.createPipelineService import (
    CreatePipelineServiceRequest,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.pipelineService import (
    PipelineService,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
)
from metadata.generated.schema.entity.services.messagingService import (
    MessagingService,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

JWT = os.getenv("OPENMETADATA_JWT")

cfg = OpenMetadataConnection(
    hostPort="http://openmetadata-server:8585/api",
    authProvider="openmetadata",
    securityConfig=OpenMetadataJWTClientConfig(jwtToken=JWT),
)
m = OpenMetadata(cfg)

print("=== Database Services ===")
dbs = m.list_services(entity=DatabaseService)
for db in dbs or []:
    print(f"  {db.name.root} -> {db.fullyQualifiedName.root}")

print("\n=== Pipeline Services ===")
ps = m.list_services(entity=PipelineService)
for p in ps or []:
    ctype = p.connection.config.type.value
    print(f"  {p.name.root} (type={ctype})")
    if ctype == "CustomPipeline":
        try:
            spc = p.connection.config.sourcePythonClass
            print(f"      sourcePythonClass: {spc}")
        except:
            pass

print("\n=== Messaging Services ===")
ms = m.list_services(entity=MessagingService)
for s in ms or []:
    print(f"  {s.name.root} -> {s.fullyQualifiedName.root}")
