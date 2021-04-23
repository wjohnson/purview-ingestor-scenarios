import os
import json

from pyapacheatlas.core import AtlasEntity, AtlasProcess
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.client import PurviewClient
from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef, RelationshipTypeDef, ParentEndDef, ChildEndDef


oauth = ServicePrincipalAuthentication(
    tenant_id=os.environ.get("TENANT_ID", ""),
    client_id=os.environ.get("CLIENT_ID", ""),
    client_secret=os.environ.get("CLIENT_SECRET", "")
)
client = PurviewClient(
    account_name=os.environ.get("PURVIEW_NAME", ""),
    authentication=oauth
)

# Let's create five custom types for use in our fictional ETL and Data Source custom ingestor
# You should customize this script for your custom data source and etl tools.
# In my case, I have a table, column, a relationship between table and columns,
# stored procedure, and etl job proccess.

# The first one will be a custom database type
# First, I will give the type a name and then I want to capture
# a string value called "container" (maybe this is my custom db's schema name or something like that).
# A more advanced implementation might add Relationship Attributes
# to connect multiple entities together but I'm going to keep it simple here

custom_db = EntityTypeDef(
    name="my_custom_db",
)
custom_db.attributeDefs.append(AtlasAttributeDef(
    "container", isOptional=True, typeName="string").to_json())

# I need to represent columns in my custom database too!
# I can use the built-in column entity type as the basis.
custom_db_column = EntityTypeDef(
    name="my_custom_db_column",
    superTypes=["column"]
)

# I also need a relationship type that connects the columns to the tables
# The 'COMPOSITION' relationship Category means a table can't be deleted
# without deleting the columns first (i.e. the columns can't be orphaned).
table_column_relationship = RelationshipTypeDef(
    name="my_custom_db_table_columns",
    relationshipCategory="COMPOSITION",
    endDef1=ParentEndDef(name="columns", typeName="my_custom_db").to_json(),
    endDef2=ChildEndDef(
        name="table", typeName="my_custom_db_column").to_json()
)


# Next I'll create a process that represents stored procedures in my
# custom database.

# In this case and below I am NOT going to capture intermediate steps in the process
# but I will make the columnMapping attribute available so that I COULD
# provide the column level mappings seen in the Purview UI.

# The superType=["Process"] will automatically give the type an inputs and
# outputs attributes. This will help us capture the data sets being used
# and data coming out of our stored procedure.
custom_db_storedproc = EntityTypeDef(
    name="my_custom_db_sp",
    superTypes=["Process"]
)

custom_db_storedproc.attributeDefs.append(
    AtlasAttributeDef(
        "columnMapping", isOptional=True, typeName="string").to_json()
)

# Next I'll create a custom process entity type that will represent a 'job'
# from my ETL tool. In addition, I want to capture the lastRun that comes
# out of my ETL tool's API data.
custom_etl_job = EntityTypeDef(
    name="my_custom_etl_job",
    superTypes=["Process"]
)

custom_etl_job.attributeDefs.extend([AtlasAttributeDef(
    "lastRun", isOptional=True, typeName="string").to_json(),
    AtlasAttributeDef(
    "columnMapping", isOptional=True, typeName="string").to_json()
])


# Finally, let's upload these types and confirm that they uploaded successfully
types_results = client.upload_typedefs(
    entityDefs=[custom_db, custom_db_column,
                custom_db_storedproc, custom_etl_job],
    relationshipDefs=[table_column_relationship],
    force_update=True
)

print("Results from upload:")
print(json.dumps(types_results, indent=2))
