import xml 
import xml.dom.minidom
# NOTE: Python documentation encourages you to use defusedxml
# if you can't trust the source to be non-malicious
import json
import os

import requests

from pyapacheatlas.core import AtlasEntity, AtlasProcess
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.client import PurviewClient

# This sample demonstrates how you would parse a fictional ETL Tool's job files
# The goal is to show how you need to be able to understand your tool's config
# / scripts and then massage that data into Atlas Entities to be uploaded
# into Azure Purview.
# The steps are primarily:
## Create the relevant custom types to represent your ETL tool's pipelines and data source's types.
## Figure out how to read your tool's files and understand its structure
## Read in the ETL tool's scripts / job files
## Massage the files into Atlas Entities
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

# Now I will read the example job file into memory and try to process it
script = xml.dom.minidom.parse('./ETLTool/jobs/job002.xml')

# Next, I want to get some of the job metadata
# My xml file has some meta data on the root level
JOB_NAME = script.documentElement.getAttribute("name")
JOB_ID = script.documentElement.getAttribute("jobId")

# In my case, I've got an ETL tool that generates XML files 
# that looks like below:
# TODO
# You will need to do the research to find out what your job looks like
# and how to access it.

# I'll create re-usable functions for processing this xml format
def get_inputs(startNode):
    output = []
    # Filter the list down to just the input nodes
    inputNodes = [x for x in startNode.childNodes if isinstance(x, xml.dom.minidom.Element)][0]
    for child in inputNodes.childNodes:
        if isinstance(child, xml.dom.minidom.Element):
            _typeName = child.getAttribute("typeName")
            _tableName = child.firstChild.data
            output.append({"name":_tableName, "type":_typeName})
    return output

def get_mappings(projectionNode):
    setOfColumns = [x for x in projectionNode.childNodes if isinstance(x, xml.dom.minidom.Element) and x.tagName == "columns"][0]
    output = []
    for columnNode in setOfColumns.childNodes:
        # Only look at the individual column mappings
        if not isinstance(columnNode, xml.dom.minidom.Element):
            continue
        mappings = [c for c in columnNode.childNodes if c.nodeType != xml.dom.minidom.Node.TEXT_NODE]
        output.append({'Source':mappings[0].firstChild.data, 'Sink': mappings[1].firstChild.data})
    
    return output


def get_output(outputNode):
    # Filter the list down to just the input nodes
    singleOutputNode = [x for x in outputNode.childNodes if isinstance(x, xml.dom.minidom.Element) and x.tagName == "output"][0]
    
    _typeName = singleOutputNode.getAttribute("typeName")
    _tableName = singleOutputNode.firstChild.data

    return {"name":_tableName, "type":_typeName}


input_tables = []
output_tables = []
column_mappings = []

for node in script.getElementsByTagName("node"):
    # Is this the starting node?
    if node.getAttribute("id") == "start":
        input_tables.extend(get_inputs(node))
    elif node.getAttribute("type") == "projection":
        column_mappings.extend(get_mappings(node))
    elif node.getAttribute("type") == "sink":
        output_tables.append(get_output(node))
    
print("Looking at the results of parsing")
print(input_tables)
print(column_mappings)
print(output_tables)

# Now I am in the Atlas Entities / Purview space!

# I'm going to include a reference to the type names I'll
# be using. 
PROCESS_TYPE_NAME = "my_custom_etl_job"

# I will start by setting up a guidtracker to generate unique
# "dummy guids" (negative numbers) that coordinate our upload
# to purview.
gt = GuidTracker()
# Now I create a list that will be used for storing our entities
entities = []

# Since we are taking a given job from our ETL tool, we will
# represent it as a single Process entity with inputs and
# outputs.  We are NOT going to represent intermediate datasets
# but WILL include column mappings in this case but you could implement
# intermediate datasets if your ETL tool provides it.

proc = AtlasProcess(
    # You might generate the  name programmatically from the job response
    name=JOB_NAME,
    guid=gt.get_guid(),
    # We need to carefully consider the qualified name pattern
    # so that it's unique, might represent a hierarchy of objects,
    # and could be generated programmatically
    qualified_name="custom://" + JOB_ID,
    typeName=PROCESS_TYPE_NAME,
    inputs=[],
    outputs=[],
    attributes={}
)

# I'll create a function that I can re-use when iterating
# over the job response's.
def create_entity_from_job_schema(job_object):
    # Now we need to create entities for each input
    # Based on my fictional job, I'm expecting an object that
    # will always have a field of name and type. Any additional
    # fields will be dependent on the type and I'll need to code
    # around that. Again, this is FICTIONAL and you will have to
    # implement this mapping in your own way for your own ETL
    # tool's job.

    # The qualified name and typename will be different
    # based on the type response of the job
    qualified_name = ""
    typeName = "DataSet"
    # We need to map our qualified name 
    if job_object["type"] == "blob":
        # In this case , the etl tool provides blob storage
        # path which happens to be the correct qualified name!
        qualified_name = job_object["name"]
        typeName = "azure_blob_path"

    elif job_object["type"] == "customDB":
        # In this case, I have to implement a custom qualified
        # name because this is the proprietary type from my etl tool
        qualified_name = "customDB://"+job_object["name"]
        typeName = "my_custom_db"

    _ae = AtlasEntity(
        name=job_object["name"],
        qualified_name=qualified_name,
        typeName=typeName,
        guid=gt.get_guid()
    )
    return _ae

for inp in input_tables:
    # We have inputs to our ETL process
    _ae = create_entity_from_job_schema(inp)
    # Now I'll add this as an input to the job process
    proc.addInput(_ae)
    entities.append(_ae)

for outp in output_tables:
    # We have outputs from our ETL process
    
    _ae = create_entity_from_job_schema(outp)
    # Now I'll add this as an output to the job process
    proc.addOutput(_ae)
    entities.append(_ae)


# Column Mappings can be complex!
output_qualified_name = (proc.outputs[0]["qualifiedName"])

COLUMN_MAPPING_PRE = {}

# First I have to collect all of the column mappings for each
# source and sink. In this case, there's only one sink but multiple
# sources.
for colmap in column_mappings:
    source_table, source_column = colmap["Source"].split(".", maxsplit=1)
    sink_column = colmap["Sink"]
    _existing_mapping = COLUMN_MAPPING_PRE.get(source_table, [])
    _existing_mapping.append(  {"Source": source_column, "Sink": sink_column} )
    COLUMN_MAPPING_PRE[source_table] = _existing_mapping

COLUMN_MAPPING = {}
for source_table, colmap in COLUMN_MAPPING_PRE.items():
    # I need to look up the source table's qualified name
    source_table_qn = [e.qualifiedName for e in entities if e.name == source_table][0]
    COLUMN_MAPPING = [
        {
            "DatasetMapping": {"Source":source_table_qn, "Sink": output_qualified_name},
            "ColumnMapping": colmap
        }
    ]

# Now that the column mapping is complete, I'll add it as an attribute.
# This only works if my custom type has the columnMapping attribute as
# part of its definition
proc.attributes.update({"columnMapping": COLUMN_MAPPING})

# Now that I have iterated over all the inputs and outputs
# I can add the process entity to my list of entities that
# will be uploaded.
entities.append(proc)

# Perform the upload and go!
results = client.upload_entities(entities)

# Print out the results
print(json.dumps(results, indent=2))
