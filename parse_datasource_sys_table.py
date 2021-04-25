import json
import os
import re

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

# This sample demonstrates how you would parse a fictional database's
# system metadata tables and constructing the Atlas Entities.
# The goal is to show how you need to be able to understand your databases'
# system tables.

# The steps are primarily:
## Create the relevant custom types to represent your data source's types
## Get the content of your system metadata tables
## Massage the data together to prepare it for Purview
## Create an Atlas Process to represent your stored procedure
## Massage the inputs and outputs into Atlas Entities
## Upload the entities

# First, I need to read in the system table. In my case, I just have a
# file but you might have to query your system table through your data source
# with tools like pyodbc to query a database.
with open('./DataSource/myCustomDatabase/sys.json') as fp:
    system_table = json.load(fp)


# I will start by setting up a guidtracker to generate unique
# "dummy guids" (negative numbers) that coordinate our upload
# to purview.
gt = GuidTracker()

# I'm also going to include a reference to the type names I'll
# be using. 
TABLE_TYPE_NAME = "my_custom_db"
COLUMN_TYPE_NAME = "my_custom_db_column"

# Now I create a list that will be used for storing our entities
entities = []

# I want to iterate over every table in my system table
for table_name, table_object in system_table["tables"].items():
    _tbl = AtlasEntity(
        name=table_name,
        guid=gt.get_guid(),
        # Your qualified name pattern may include server, database, container, etc. 
        # You should plan this out carefully.
        qualified_name="custom://{}".format(table_name), 
        typeName=TABLE_TYPE_NAME,
        attributes={"description": table_object["description"]}  # Add any custom attributes
    )
    entities.append(_tbl)

    # Are there columns here?
    if len(system_table["columns"][table_name]) > 0:
        for col in system_table["columns"][table_name]:
            # Add each column as an entity
            _c = AtlasEntity(
                name=col["name"],
                guid=gt.get_guid(),
                # Your qualified name pattern may include server, database, container, etc. 
                # You should plan this out carefully.
                # Typically, it's <table qualified name>#<column name> for columns
                qualified_name="custom://{}#{}".format(table_name, col["name"]),
                typeName=COLUMN_TYPE_NAME,
                # Capture some additional attributes here from your script
                attributes={
                    # I'm passing in the description and type I found in the
                    # system table.
                    "type": col["type"],
                    "description": col["description"]
                } 
            )
            # Add a relationship attribute that connects the column to the table
            # This "table" relationship attribute must be defined in your 
            # custom type.
            _c.addRelationship(table=_tbl)
            entities.append(_c)


# Perform the upload and go!
results = client.upload_entities(entities)

# Print out the results
print(json.dumps(results,indent=2))
