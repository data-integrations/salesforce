# Salesforce Marketing Cloud Data Extension Sink


Description
-----------
This sink inserts records into a Salesforce Marketing Cloud Data Extension.
The sink requires Server-to-Server integration with the Salesforce Marketing Cloud API. See
https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/api-integration.htm
for more information about creating an API integration.

Records are written in batches to the Data Extension. By default, any errors inserting the
records will be logged and ignored, though the sink can be configured to fail the pipeline if
it encounters an error. However, insertions are not atomic, so a failure to write a record
midway through the pipeline will result in partial writes being made to the Data Extension.

Input fields must be named the same as they are named in the Data Extension.
Input fields must be of a type compatible with the column type in the Data Extension.

  - If the column type is text, phone, email, or locale, the input field must be a string
  - If the column type is boolean, the input field must be a boolean or a string representing a boolean.
  - If the column type is date, the input field must be a date or a string representing a date in a supported format
  - If the column type is number, the input field must be an int or a string representing an int
  - If the column type is a decimal, the input field must be a decimal or a string representing a decimal

The sink can be configured to either insert records or update records.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Data Extension:** Data Extension to write to.

**Operation:** Whether to insert or update records in the Data Extension.

**Client ID:** OAuth2 client ID associated with an installed package in the Salesforce Marketing Cloud.

**Client Secret:** OAuth2 client secret associated with an installed package in the Salesforce Marketing Cloud.

**Authentication Base URI:** Authentication Base URL associated for the Server-to-Server API integration.

**SOAP Base URI:** Authentication Base URL associated for the Server-to-Server API integration.

**Max Batch Size:** Maximum number of records to batch together in a write. Batching is used to improve
write performance. Records in a batch are not applied atomically. This means some records in a batch
may be written successfully while others in the batch may fail.

**Fail On Error:** Whether to fail the pipeline if an error is encountered while inserting records into
the Data Extension.
