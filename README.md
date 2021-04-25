# Creating Your Own Purview Custom Connector

This sample provides several examples and tips on creating your own custom connector / "ingestor" for an unsupported data source or etl tool that you would like to see in Azure Purview.  All of the examples are on a fictional data source or etl tool and serve only as a way to illustrate how you *might* go about this process. The solutions provided are not production ready and should be considered for inspirational purposes only.

## Decision Tree / Summary

* Plan your custom types
  * ETL Tools: How do you represent a job, pipeline, activity?
  * Data Source: How do you represent its hierarchy like server, containers, tables, and columns?
  * Plan your Qualified Name pattern
* Do you want to "ingest" a Data Source / Database?
  * Does the Data Source have a system / management table?
  * If No, are you able to crawl the data source in some way?
  * Do you need to handle custom code (like stored procedures)?
* Do you want to "ingest" a custom ETL tool?
  * Does the ETL tool have an API that you can use?
  * If No, can you extract configuration or log files from ETL job runs?
* Consider using an Excel upload when you can't invest in writing code

## Planning your Custom Types
The first thing to consider is your **qualified name** pattern. In many of the Azure built-in types, they follow either a URL pattern (as in blob storage's qualified name is just its full URL path) or there is some hierarchy that is prefixed by a type identifier (e.g. mssql://server.database.schema.table#column).

It's important to be able to programmatically generate your qualified name from an operations perspective. You'll often need to use the qualified name to uniquely identify an entity. Otherwise you'd have to look up the guid each time or carefully search for the entity.

Once you have your qualified name pattern decided, you'll need to determine:
* How many types are you capturing (server, database, schema, table, column or just the table)?
* What attributes you want to capture for each type (especially required attributes)?
* What "relationships" you want to establish between types (e.g. representing a table and columns relationship or a server and database relationship)?

After designing those entity types and relationship types, upload them to your Purview account and you're now ready for feeding custom entities to Purview. For an example of this, see `custom_types_for_ingestor.py`.

## Extract from a Data Source

In the best case, your data source has some system table that you can query and extract the relevant metadata from (table names, columns, any hierarchical relationships you want to cover). See `parse_datasource_sys_table.py` for a fictional example of this.

In the worst case, you'll need to crawl your data source (like a file system) yourself.

In addition, if you have any custom querying tool built into your data source (like a stored procedure in a database), you'll need to figure out how to parse that code or read its execution history. See `parse_custom_sp.py` for a fictional example of this.

## Extract from an ETL Tool

In the best case, your ETL tool has a built-in API that you can extract data programmatically. In this case, it's probably a good idea to mirror your types based on their API. In addition, you need to determine if you're going to capture only physical tables or you want to capture intermediate steps (see Purview sample for Workflow Process steps as an option). See `parse_etlserver_api.py` for a fictional example of this.

If your ETL tool does NOT provide an API for you to use, you may need to parse the scripts that are generated from its UI. Perhaps these are some SQL scripts or in a proprietary format. In either case, you'll need to parse the job's file and determine inputs and outputs for the job. See `parse_etlserver_jobfile.py` for a fictional example of this.

## When You Don't Want to Code Anything

A completely valid solution is to avoid writing any code. Often, ETL teams are already capturing this lineage information in Excel spreadsheets. As a result, you might use the Purview REST API to load data based on a spreadsheet. There are several [PyApacheAtlas samples for Excel](https://github.com/wjohnson/pyapacheatlas/tree/master/samples/excel) that take advantage of its [Excel Template and Parsing features](https://github.com/wjohnson/pyapacheatlas/wiki/Excel-Template-and-Configuration).

## Scheduling your Ingestor and Production Tips

Since this is your ingestor, you are responsible for creating a schedule and automating that ingestion.  Windows Task Scheduler and Crontab are two options you should consider.

* [Windows Task Scheduler](https://docs.microsoft.com/en-us/windows/win32/taskschd/task-scheduler-start-page)
* [Linux Crontab](https://help.ubuntu.com/community/CronHowto)

In addition, you should consider a few additional steps to make sure your ingestor is production ready.

* Watermarking: Consider maintaining state in Azure Blob Storage, Azure SQL DB, on a local database with backups. This state would indicate where your previous scan left off so that you don't have to waste time scanning every asset over and over again.
* Storing secrets: Consider using a service like Azure Key Vault to house your service principal credentials. Enabling an Azure VM to access the Key Vault and pull down the Service Principals' credentials may be a better solution than storing the credentials in plain text as environment variables as in these examples.