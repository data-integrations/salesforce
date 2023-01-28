# Salesforce Batch Multi Source


Description
-----------
This source reads multiple sObjects from Salesforce. The data which should be read is specified using list of sObjects 
and incremental or range date filters. The source will output a record for each row in the SObjects it reads, 
with each record containing an additional field that holds the name of the SObject the record came from. 
In addition, for each SObject that will be read, this plugin will set pipeline arguments where the key is 'multisink.[SObjectName]' 
and the value is the schema of the SObject.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Salesforce password.

**Security Token:** Salesforce security Token. If the password does not contain the security token the plugin 
will append the token before authenticating with salesforce.

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client id.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login Url:** Salesforce OAuth2 login url.

**Connect Timeout:** Maximum time in milliseconds to wait for connection initialization before time out.

**White List**: List of SObjects to read from. By default all SObjects will be white listed. 
For each white listed SObject, a SOQL query will be generated of the form:
`select <FIELD_1, FIELD_2, ..., FIELD_N> from ${sObjectName}`.

**Black List**: List of SObjects to avoid reading from. By default NONE of SObjects will be black listed.

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

**Query Operation:**
Specify the query operation to run on the table. If query is selected, only current records will be returned.
If queryAll is selected, all current and deleted records will be returned. Default operation is query.

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

**SObject Name Field**: The name of the field that holds the SObject name. 
Must not be the name of any SObject column that will be read. Defaults to `tablename`.
    
Example
----------

There are two SObjects of interest in Salesforce.
The first SObject is named 'Account' and contains:

| Id  | Name     | Email            |
| --- | -------- | ---------------- |
| 0   | Samuel   | sjax@example.net |
| 1   | Alice    | a@example.net    |

The second is named 'Activity' and contains:

| Id     | Item     | Action |
| ------ | -------- | ------ |
| 0      | shirt123 | view   |
| 0      | carxyz   | view   |
| 0      | shirt123 | buy    |
| 0      | coffee   | view   |
| 1      | cola     | buy    |
    
To read data from these two SObjects, both of them must be indicated in the `White List` configuration property.

The output of the the source will be the following records:

| Id  | Name     | Email            | tablename |
| --- | -------- | ---------------- | --------- |
| 0   | Samuel   | sjax@example.net | Account   |
| 1   | Alice    | a@example.net    | Account   |


| Id     | Item     | Action | tablename |
| ------ | -------- | ------ | --------- |
| 0      | shirt123 | view   | Activity  |
| 0      | carxyz   | view   | Activity  |
| 0      | shirt123 | buy    | Activity  |
| 0      | coffee   | view   | Activity  |
| 1      | cola     | buy    | Activity  |
    
The plugin will emit two pipeline arguments to provide multi sink plugin with the schema of the output records:

```js
multisink.Account =
  {
    "type": "record",
    "name": "output",
    "fields": [
      { "name": "Id", "type": "long" } ,
      { "name": "Name", "type": "string" },
      { "name": "Email", "type": [ "string", "null" ] }
    ]
  }
multisink.Activity =
  {
    "type": "record",
    "name": "output",
    "fields": [
      { "name": "Id", "type": "long" } ,
      { "name": "Item", "type": "string" },
      { "name": "Action", "type": "string" }
    ]
  }
```

There are also **sObjects** that are not supported in the Bulk API of Salesforce.
When a job is created using an object that is not supported in the Bulk API, "_Entity is not supported by the Bulk API_" is thrown.
These objects are also not supported by _Einstein Analytics_ as it also uses Bulk API for querying data.

Below is a non-comprehensive list of **sObjects** that are not currently available in the Bulk API:
- *Feed (e.g. AccountFeed, AssetFeed, ...)
- *Share (e.g. AccountBrandShare, ChannelProgramLevelShare, ...)
- *History (e.g. AccountHistory, ActivityHistory, ...)
- *EventRelation (e.g. AcceptedEventRelation, DeclinedEventRelation, ...)
- AggregateResult
- AttachedContentDocument
- CaseStatus
- CaseTeamMember
- CaseTeamRole
- CaseTeamTemplate
- CaseTeamTemplateMember
- CaseTeamTemplateRecord
- CombinedAttachment
- ContentFolderItem
- ContractStatus
- EventWhoRelation
- FolderedContentDocument
- KnowledgeArticleViewStat
- KnowledgeArticleVoteStat
- LookedUpFromActivity
- Name
- NoteAndAttachment
- OpenActivity
- OwnedContentDocument
- PartnerRole
- RecentlyViewed
- ServiceAppointmentStatus
- SolutionStatus
- TaskPriority
- TaskStatus
- TaskWhoRelation
- UserRecordAccess
- WorkOrderLineItemStatus
- WorkOrderStatus
