# Salesforce Batch Source


Description
-----------
This source reads sObjects from Salesforce.
Examples of sObjects are opportunities, contacts, accounts, leads, any custom object, etc.

The data which should be read is specified using SOQL queries (Salesforce Object Query Language queries)
or using sObject and incremental or range date filters.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Salesforce password.

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client id.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login Url:** Salesforce OAuth2 login url.

**SOQL Query:** A SOQL query to fetch data into source.

Examples:

``SELECT Id, Name, BillingCity FROM Account``

``SELECT Id FROM Contact WHERE Name LIKE 'A%' AND MailingCity = 'California'``

**SObject Name:** Salesforce object name to read. If value is provided, plugin will get all fields for this object from 
Salesforce and generate SOQL query (`select <FIELD_1, FIELD_2, ..., FIELD_N> from ${sObjectName}`). 
Ignored if SOQL query is provided. 

**Last Modified After:** Filter data to only include records where the system field `LastModifiedDate` is greater than 
or equal to the specified date. The date must be provided in the Salesforce date format:

|              Format              |       Format Syntax       |          Example          |
| -------------------------------- | ------------------------- | ------------------------- |
| Date, time, and time zone offset | YYYY-MM-DDThh:mm:ss+hh:mm | 1999-01-01T23:01:01+01:00 |
|                                  | YYYY-MM-DDThh:mm:ss-hh:mm | 1999-01-01T23:01:01-08:00 |
|                                  | YYYY-MM-DDThh:mm:ssZ      | 1999-01-01T23:01:01Z      |

If no value is provided, no lower bound for LastModifiedDate is applied.

**Last Modified Before:** Filter data to only include records where the system field `LastModifiedDate` is less than 
the specified date. The date must be provided in the Salesforce date format:

|              Format              |       Format Syntax       |          Example          |
| -------------------------------- | ------------------------- | ------------------------- |
| Date, time, and time zone offset | YYYY-MM-DDThh:mm:ss+hh:mm | 1999-01-01T23:01:01+01:00 |
|                                  | YYYY-MM-DDThh:mm:ss-hh:mm | 1999-01-01T23:01:01-08:00 |
|                                  | YYYY-MM-DDThh:mm:ssZ      | 1999-01-01T23:01:01Z      |

Specifying this along with `Last Modified After` allows reading data modified within a specific time window. 
If no value is provided, no upper bound for `LastModifiedDate` is applied.

**Duration:** Filter data read to only include records that were last modified within a time window of the specified size. 
For example, if the duration is '6 hours' and the pipeline runs at 9am, it will read data that was last updated 
from 3am (inclusive) to 9am (exclusive). The duration is specified using numbers and time units:

|  Unit   |
| ------- |
| SECONDS |
| MINUTES |
| HOURS   |
| DAYS    |
| MONTHS  |
| YEARS   |
              
Several units can be specified, but each unit can only be used once. For example, `2 days, 1 hours, 30 minutes`.
The duration is ignored if a value is already specified for `Last Modified After` or `Last Modified Before`.

**Offset:** Filter data to only read records where the system field `LastModifiedDate` is less than the logical start time 
of the pipeline minus the given offset. For example, if duration is '6 hours' and the offset is '1 hours', and the pipeline 
runs at 9am, data last modified between 2am (inclusive) and 8am (exclusive) will be read. 
The duration is specified using numbers and time units:

|  Unit   |
| ------- |
| SECONDS |
| MINUTES |
| HOURS   |
| DAYS    |
| MONTHS  |
| YEARS   |

Several units can be specified, but each unit can only be used once. For example, `2 days, 1 hours, 30 minutes`.
The offset is ignored if a value is already specified for `Last Modified After` or `Last Modified Before`.

**Schema:** The schema of output objects.
The Salesforce types will be automatically mapped to schema types as shown below:

| Schema type |                              Salesforce type                               |    Notes     |
| ----------- | -------------------------------------------------------------------------- | ------------ |
| bool        | _bool                                                                      |              |
| int         | _int                                                                       |              |
| long        | _long                                                                      |              |
| double      | _double, currency, percent                                                 |              |
| date        | date                                                                       |              |
| timestamp   | datetime                                                                   | Microseconds |
| time        | time                                                                       | Microseconds |
| string      | picklist, multipicklist, combobox, reference, base64,                      |              |
|             | textarea, phone, id, url, email, encryptedstring,                          |              |
|             | datacategorygroupreference, location, address, anyType, json, complexvalue |              |

