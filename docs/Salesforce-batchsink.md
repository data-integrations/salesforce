# Salesforce Batch Sink


Description
-----------
A batch sink that inserts sObjects into Salesforce.
Examples of sObjects are opportunities, contacts, accounts, leads, any custom objects, etc.

Currently, only inserts are supported. Upserts are not supported.

Configuration
-------------

**Use Connection:** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection:** Name of the connection to use. Object Names information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.  

**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Salesforce password.

**Security Token:** Salesforce security token. If the password does not contain the security token, the plugin 
will append the token before authenticating with Salesforce.

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client  ID.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login URL:** Salesforce OAuth2 login URL.

**Connect Timeout:** Maximum time in milliseconds to wait for connection initialization before it times out.

**SObject Name:** Salesforce object name to insert records into.

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

**Operation:** Operation used for writing data into Salesforce.  
Insert - adds records.  
Upsert - upserts the records. Salesforce will decide if sObjects 
are the same using external ID field.  
Update - updates existing records based on Id field.

**Upsert External ID Field:** External id field name. It is used only if operation is upsert.
The field specified can be either 'Id' or any customly created field, which has external id attribute set.

**Concurrency Mode:** The concurrency mode for the bulk job. Select one of the following options:  
Parallel - Process batches in parallel mode.  
Serial - Process batches in serial mode. Processing in parallel can cause lock contention. When this is severe, 
the Salesforce job can fail. If you’re experiencing this issue, in the Salesforce sink, change concurrency mode to 
Serial and run the pipeline again. This mode guarantees that batches are processed one at a time, but can 
significantly increase the processing time.  
Default is Parallel.

**Max Records Per Batch:** Maximum number of records to include in a batch when writing to Salesforce.
This value cannot be greater than 10,000.

**Max Bytes Per Batch:** Maximum size in bytes of a batch of records when writing to Salesforce.
This value cannot be greater than 10,000,000.

**Error Handling:** Strategy used to handle erroneous records.  
Skip on error - Ignores erroneous records.  
Fail on error - Fails pipeline due to erroneous record.
