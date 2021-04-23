import json
import os

import requests

from pyapacheatlas.core import AtlasEntity, AtlasProcess
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.client import PurviewClient

oauth = ServicePrincipalAuthentication(
    tenant_id=os.environ.get("TENANT_ID", ""),
    client_id=os.environ.get("CLIENT_ID", ""),
    client_secret=os.environ.get("CLIENT_SECRET", "")
)
client = PurviewClient(
    account_name=os.environ.get("PURVIEW_NAME", ""),
    authentication=oauth
)


results = requests.get("http://localhost:8088/api/job/001")

response_json = results.json()

print(json.dumps(response_json,indent=2))
print(type(response_json))
PROCESS_TYPE_NAME = "demo_process"
TABLE_TYPE_NAME = "demo_table"

#
gt = GuidTracker()
entities = []
proc = AtlasProcess(
    name="sp_transform_job.custom",
    guid=gt.get_guid(),
    qualified_name="custom://sp_transform_job.custom",
    typeName=PROCESS_TYPE_NAME,
    inputs=[],
    outputs=[],
    attributes={}
)

for inp in response_json["inputs"]:
    # We have inputs to our ETL process
    # Now we need to create entities for each input
    # The qualified namd and typename will be different
    # based on the type response of the API
    qualified_name = ""
    typeName = "DataSet"
    # We need to map our qualified name 
    if inp["type"] == "blob":
        # In this case , the etl tool provides blob storage
        # path which happens to be the correct qualified name!
        qualified_name = inp["path"]
        typeName = "azure_blob_storage"

    elif inp["type"] == "customDB":
        # In this case, I have to implement a custom qualified
        # name because this is the proprietary type from my etl tool
        qualified_name = "customDB://"+inp["name"]
        typeName = "my_custom_db"

    _ae = AtlasEntity(
        name=inp["name"],
        qualified_name=qualified_name,
        typeName=typeName,
        guid=gt.get_guid()
    )
    proc.addInput(_ae)
    entities.append(_ae)

for outp in response_json["outputs"]:
    # We have inputs to our ETL process
    # Now we need to create entities for each input
    # The qualified namd and typename will be different
    # based on the type response of the API
    qualified_name = ""
    typeName = "DataSet"
    # We need to map our qualified name 
    if outp["type"] == "blob":
        # In this case , the etl tool provides blob storage
        # path which happens to be the correct qualified name!
        qualified_name = outp["path"]
        typeName = "azure_blob_storage"

    elif outp["type"] == "customDB":
        # In this case, I have to implement a custom qualified
        # name because this is the proprietary type from my etl tool
        qualified_name = "customDB://"+outp["name"]
        typeName = "my_custom_db"

    _ae = AtlasEntity(
        name=outp["name"],
        qualified_name=qualified_name,
        typeName=typeName,
        guid=gt.get_guid()
    )
    proc.addInput(_ae)
    entities.append(_ae)


entities.append(proc)

print(entities)