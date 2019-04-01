# Salesforce Batch Source


Description
-----------
This source reads sObjects from Salesforce.
Examples of sObjects are opportunities, contacts, accounts, leads, any custom object, etc.

The data which should be read is specified using SOQL queries (Salesforce Object Query Language queries)

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Saleforce password.

**Client Id:** Application Client Id. This is also called "Consumer Key" in Salesforce UI.
To create Client Id user needs to create a connected application in Salesforce first.

**Client Secret:** Application Client Secret. This is also called "Consumer Secret" in Salesforce UI.
To create Client Secret user needs to create a connected application in Salesforce first.

**Login Url:** Salesforce OAuth2 login url.

**SOQL Query:** A SOQL query to fetch data into source. Aliases in queries are not supported

Examples:

``SELECT Id, Name, BillingCity FROM Account``

``SELECT Id FROM Contact WHERE Name LIKE 'A%' AND MailingCity = 'California'``

**Error Handling:** Strategy used to handle erroneous records. Acceptable values are Skip on error,
Send to error, Stop on error.

| Value  | Description |
| ------ | ----------- |
| Skip on error | Ignores erroneous records  |
| Send to error | Emits an error to error handler. Errors are records with a field 'body', containing erroneous row. |
| Stop on error | Fails pipeline due to erroneous record |

**Schema:** The schema of output objects.
The Salesforce types will be automacitally mapped to schema types as shown below:


| Schema type  | Salesforce type | Notes |
| ------------- | ------------- | ------------- |
| bool  | _bool | |
| int | _int | |
| long | _long | |
| double| _double, currency, percent | |
| date | date | |
| timestamp | datetime | Milliseconds |
| time | time | Milliseconds |
| string | picklist, multipicklist, combobox, reference, base64,<br>textarea, phone, id, url, email, encryptedstring,<br>datacategorygroupreference, location, address, anyType, json, complexvalue	| |
