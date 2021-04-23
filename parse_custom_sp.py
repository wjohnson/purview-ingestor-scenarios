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
# stored procedure logic by parsing the actual text and constructing
# the Atlas Entities.
# The goal is to show how you need to be able to understand your databases'
# syntax.  However, this is not a full lexer / parser. If you're going
# to get serious about this, there are lots of great tutorials out there.

# The steps are primarily:
## Create the relevant custom types to represent your data source's types.
## Get the content of your stored procedure
## Parse the various commands to determine the inputs and outputs
## Create an Atlas Process to represent your stored procedure
## Massage the inputs and outputs into Atlas Entities
## Upload the entities

# First, I need to read in a stored process. In my case, I just have a
# file but you might have to query your stored proc through your data source
with open('./DataSource/myCustomDatabase/sp_transform_job.custom') as fp:
    script = fp.readlines()

# My custom database allows for aliasing datasets so my script needs
# to be able to identify this alias.
ALIASES = dict()
# I'm going to hold all of the inputs, outputs, and intermediate datasets
# in this variable so I can iterate over it and later create all of the
# Atlas Entities.
DATASETS = {
}

# Now I have to actually parse each line of the script.
# All of this parsing is completely CUSTOM to my data source / stored proc syntax.
# As a result, the actual processing done here is likely not useful to you but
# you can use the concepts / steps in your own implementation.

# I will iterate over each line (and include a line number for debugging)
for rownum, line in enumerate(script):
    if line == "" or line == "\n":
        continue
    # Each line must contain a variable declaration and operation separated by an equal symbol
    variable, *operation = line.split("=", maxsplit=1)

    operation = operation[0]
    if not(operation.startswith('(') and operation.endswith(')\n')):
        # The operation is malformed and we should error out
        raise ValueError(
            "Line {}: The operation is malformed: {}".format(rownum, operation))

    # These should be wrapped in parens, so cut off the first and last characters
    operation = operation[1:-2]
    arguments = operation.split(",")
    # Big If condition block to see what operation we're working with
    function_argument = arguments[0]
    input_arguments = []
    # Handle internal aliasing of tables
    for idx, arg in enumerate(arguments[1:]):
        output = arg
        if arg in ALIASES:
            output = ALIASES[arg]
        input_arguments.append(output)

    # Now that I have the functions and operations parsed out, I
    # need to write code on how to handle each function!
    if function_argument == "ALIAS":
        # Example: X=(ALIAS,myTable)
        # X is our variable and myTable is our first input argument
        ALIASES[variable] = input_arguments[0]

    elif function_argument == "PROJECT":
        # A projection indicates we are creating an intermediate dataset
        _source = input_arguments[0]
        _columns = input_arguments[1:]
        DATASETS[variable] = {"columns": _columns, "source": _source}

    elif function_argument == "GROUPED_SUM":
        # A grouped sum indicates we are aggregating a dataset
        _source = input_arguments[0]
        _aggregate_column = input_arguments[1:2]
        # These should be wrapped in parens, so cut off the first and last characters
        _group_by_columns = input_arguments[2:]
        DATASETS[variable] = {
            "columns": _group_by_columns + _aggregate_column, "source": _source}

    elif function_argument == "READ":
        # Reading a file from the database
        _source = input_arguments[0]
        _columns = input_arguments[1:]
        DATASETS[variable] = {"columns": _columns, "source": "*"}

    elif function_argument == "WRITE":
        # Writing a file to the database
        _destTable = input_arguments[0]
        _source = input_arguments[1]
        DATASETS[variable] = {"columns": [],
                              "source": _source, "destination": _destTable}
    else:
        raise NotImplementedError(
            "Line {}: Function {} is not supported".format(rownum, function_argument))

# Printing out the parsed content of the stored proc for debugging
print("Parsed content from stored procedure:")
print(json.dumps(DATASETS, indent=2))

# Now that I've parsed the script, I can create the entities
# Creating a Process to represent the stored proc
# I will start by setting up a guidtracker to generate unique
# "dummy guids" (negative numbers) that coordinate our upload
# to purview.
gt = GuidTracker()

# I'm also going to include a reference to the type names I'll
# be using. 
PROCESS_TYPE_NAME = "my_custom_db_sp"
TABLE_TYPE_NAME = "my_custom_db"
COLUMN_TYPE_NAME = "my_custom_db_column"

# Now I create a list that will be used for storing our entities
entities = []
# This process will represent the stored procedure as part of the lineage of these tables
proc = AtlasProcess(
    # You should programmatically generate this
    name="sp_transform_job.custom",
    guid=gt.get_guid(),
    # You should programmatically generate this
    qualified_name="custom://sp_transform_job.custom",
    typeName=PROCESS_TYPE_NAME,
    inputs=[],
    outputs=[],
    attributes={}
)

# Now that we have parsed the proprietary code of this data source
# We need to iterate over the results: .items() gives both the key
# and value of a dictionary. In this case, that means we get the
# table name and table definition.
for table, current_table_definition in DATASETS.items():
    # In my parsing, I wrote a source of '*' to represent an input dataset
    is_intermediate_table = (current_table_definition["source"] != "*")
    # The custom code indicates an output line with @@OUTPUT as the variable name
    is_output_table = (table == "@@OUTPUT")
    # In this case, I don't care about the intermediate results
    # If you cared about tracking the transforms at the column
    # level, you would need to create some means of tracking
    # transformations at the column level
    # For ease of this demonstration, I'm keeping it simple
    if is_intermediate_table and not is_output_table:
        continue # Skipping this intermediate result
    
    # Since the output lines store the table name in "destination"
    # We need to update the table_name variable with the contents
    # of the "destination" field.
    table_name = table
    if is_output_table:
        table_name = current_table_definition["destination"]
        # In addition, the output line doesn't carry the columns
        # We need to look up the columns from its source and
        # update our definition 
        _source = current_table_definition["source"]
        _source_definition = DATASETS[_source]
        current_table_definition["columns"] = _source_definition["columns"]

    _tbl = AtlasEntity(
        name=table_name,
        guid=gt.get_guid(),
        # Your qualified name pattern may include server, database, container, etc. 
        # You should plan this out carefully.
        qualified_name="custom://{}".format(table_name), 
        typeName=TABLE_TYPE_NAME,
        attributes={}  # Add any custom attributes
    )
    entities.append(_tbl)

    # Are there columns here?
    if len(current_table_definition["columns"]) > 0:
        for col in current_table_definition["columns"]:
            # Add each column as an entity
            _c = AtlasEntity(
                name=col,
                guid=gt.get_guid(),
                # Your qualified name pattern may include server, database, container, etc. 
                # You should plan this out carefully.
                # Typically, it's <table qualified name>#<column name> for columns
                qualified_name="custom://{}#{}".format(table_name, col),
                typeName=COLUMN_TYPE_NAME,
                # Capture some additional attributes here from your script
                attributes={
                    # I'm passing in a default of string, but your code
                    # should pass in the real type!
                    "type": "string"
                } 
            )
            # Add a relationship attribute that connects the column to the table
            # This "table" relationship attribute must be defined in your 
            # custom type.
            _c.addRelationship(table=_tbl)
            entities.append(_c)
    
    if is_output_table:
        proc.addOutput(_tbl)
    else:
        proc.addInput(_tbl)

# Finally, we add the process entity to our list of entities to upload
entities.append(proc)

# Perform the upload and go!
results = client.upload_entities(entities)

# Print out the results
print(json.dumps(results,indent=2))
