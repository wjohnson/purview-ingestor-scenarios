import json
import os

import requests

from pyapacheatlas.core import AtlasEntity, AtlasProcess
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.client import PurviewClient

# This sample demonstrates how you would parse a fictional ETL Tool's API
# The goal is to show how you need to be able to understand your tool's
# API and then massage that data into Atlas Entities to be uploaded
# into Azure Purview.
# The steps are primarily:
## Create the relevant custom types to represent your ETL tool's pipelines and data source's types.
## Figure out how to query your tool's API and understand its structure
## Design a set of custom types (see `custom_types_for_ingestor.py`) for your ETL tool and its data sets.
## Execute your queries against the tools' API
## Massage the response into Atlas Entities
## Upload the entities


# First we need to log in with our Azure Purview Credentials
oauth = ServicePrincipalAuthentication(
    tenant_id=os.environ.get("TENANT_ID", ""),
    client_id=os.environ.get("CLIENT_ID", ""),
    client_secret=os.environ.get("CLIENT_SECRET", "")
)
client = PurviewClient(
    account_name=os.environ.get("PURVIEW_NAME", ""),
    authentication=oauth
)

# Now we can call our API. In this case, the server is running
# locally but you would need to figure out authentication and
# the end point for your real server.

# Here, I'm issuing a GET request the the 'job' api and getting
# the job with id # 001.  I'll store the json data into a variable
# so that we can work with it further below.
results = requests.get("http://localhost:8088/api/job/001")
response_json = results.json()

# In my case, I've got an ETL tool that will return a response
# that looks like below:
# {
#     "name": "my-daily-etl-job",
#     "lastRun": "yesterday",
#     "inputs": [ {"name":"...", "type":"...", "...extraparams...":"..."}],
#     "outputs": [ {"name":"...", "type":"...", "...extraparams...":"..."}]
# }
# You will need to do the research to find out what your api looks like
# and how to access it.

print("Response from ETL tool API:")
print(json.dumps(response_json,indent=2))
print()

# I'm also going to include a reference to the type names I'll
# be using. 
PROCESS_TYPE_NAME = "demo_process"
TABLE_TYPE_NAME = "demo_table"

# Now I am in the Atlas Entities / Purview space!
# I will start by setting up a guidtracker to generate unique
# "dummy guids" (negative numbers) that coordinate our upload
# to purview.
gt = GuidTracker()
# Now I create a list that will be used for storing our entities
entities = []

# Since we are taking a given job from our ETL tool, we will
# represent it as a single Process entity with inputs and
# outputs.  We are NOT going to represent intermediate datasets
# or column transformations in this case but you could implement
# this if your ETL tool provides it.

proc = AtlasProcess(
    # You might generate the  name programmatically from the API response
    name=response_json["name"],
    guid=gt.get_guid(),
    # We need to carefully consider the qualified name pattern
    # so that it's unique, might represent a hierarchy of objects,
    # and could be generated programmatically
    qualified_name="custom://" + response_json["name"],
    typeName=PROCESS_TYPE_NAME,
    inputs=[],
    outputs=[],
    attributes={}
)

# I'll create a function that I can re-use when iterating
# over the API response's.
def create_entity_from_api_schema(api_object):
    # Now we need to create entities for each input
    # Based on my fictional API, I'm expecting an object that
    # will always have a field of name and type. Any additional
    # fields will be dependent on the type and I'll need to code
    # around that. Again, this is FICTIONAL and you will have to
    # implement this mapping in your own way for your own ETL
    # tool's API.

    # The qualified name and typename will be different
    # based on the type response of the API
    qualified_name = ""
    typeName = "DataSet"
    # We need to map our qualified name 
    if api_object["type"] == "blob":
        # In this case , the etl tool provides blob storage
        # path which happens to be the correct qualified name!
        qualified_name = api_object["path"]
        typeName = "azure_blob_path"

    elif api_object["type"] == "customDB":
        # In this case, I have to implement a custom qualified
        # name because this is the proprietary type from my etl tool
        qualified_name = "customDB://"+api_object["name"]
        typeName = "my_custom_db"

    _ae = AtlasEntity(
        name=api_object["name"],
        qualified_name=qualified_name,
        typeName=typeName,
        guid=gt.get_guid()
    )
    return _ae

for inp in response_json["inputs"]:
    # We have inputs to our ETL process
    _ae = create_entity_from_api_schema(inp)
    # Now I'll add this as an input to the job process
    proc.addInput(_ae)
    entities.append(_ae)

for outp in response_json["outputs"]:
    # We have outputs from our ETL process
    
    _ae = create_entity_from_api_schema(outp)
    # Now I'll add this as an output to the job process
    proc.addOutput(_ae)
    entities.append(_ae)

# Now that I have iterated over all the inputs and outputs
# I can add the process entity to my list of entities that
# will be uploaded.
entities.append(proc)

# Perform the upload and go!
results = client.upload_entities(entities)

# Print out the results
print(json.dumps(results, indent=2))
