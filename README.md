# Creating Your Own Purview Custom Connector

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


## Data Source