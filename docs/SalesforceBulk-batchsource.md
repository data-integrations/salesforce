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

**Schema:** The schema of output objects.
