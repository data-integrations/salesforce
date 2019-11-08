# Salesforce Batch Sink


Description
-----------
A batch sink that inserts sObjects into Salesforce.
Examples of sObjects are opportunities, contacts, accounts, leads, any custom objects, etc.

Currently, only inserts are supported. Upserts are not supported.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Salesforce password + security token (concatenated together with no spaces in between).

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client id.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login Url:** Salesforce OAuth2 login url.

**SObject Name:** Salesforce object name to insert records into.

**Operation:** Operation used for writing data into Salesforce.<br>
Insert - adds records.<br>
Upsert - upserts the records. Salesforce will decide if sObjects 
are the same using external id field.<br>
Update - updates existing records based on Id field.

**Upsert External ID Field:** External id field name. It is used only if operation is upsert.
The field specified can be either 'Id' or any customly created field, which has external id attribute set.

**Max Records Per Batch:** Maximum number of records to include in a batch when writing to Salesforce.
This value cannot be greater than 10,000.

**Max Bytes Per Batch:** Maximum size in bytes of a batch of records when writing to Salesforce.
This value cannot be greater than 10,000,000.

**Error Handling:** Strategy used to handle erroneous records.<br>
Skip on error - Ignores erroneous records.<br>
Stop on error - Fails pipeline due to erroneous record.
