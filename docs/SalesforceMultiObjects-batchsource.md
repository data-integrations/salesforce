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

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client id.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login Url:** Salesforce OAuth2 login url.

**White List**: List of SObjects to read from. By default all SObjects will be white listed. 
For each white listed SObject, a SOQL query will be generated of the form:
`select <FIELD_1, FIELD_2, ..., FIELD_N> from ${sObjectName}`.

**Black List**: List of SObjects to avoid reading from. By default NONE of SObjects will be black listed.

**Datetime Filter:** The amount of data to read. When a duration is specified, 
a query filter will be applied on the `LastModifiedDate` system field. 
Filter is applied in `>` (greater than) comparison only and will be added to SObject query in a form of 
where clause (`WHERE LastModifiedDate > ${datetimeFilter}`). 
If value is not provided, it means all records to be read since the beginning of time. 
The filter can be one of two types: 

`SOQL Date Format` - string should be in Salesforce Date Formats. 

    +----------------------------------+---------------------------+---------------------------+
    |              Format              |       Format Syntax       |          Example          |
    +----------------------------------+---------------------------+---------------------------+
    | Date, time, and time zone offset | YYYY-MM-DDThh:mm:ss+hh:mm | 1999-01-01T23:01:01+01:00 |
    |                                  | YYYY-MM-DDThh:mm:ss-hh:mm | 1999-01-01T23:01:01-08:00 |
    |                                  | YYYY-MM-DDThh:mm:ssZ      | 1999-01-01T23:01:01Z      |
    +----------------------------------+---------------------------+---------------------------+

`SOQL Date Literal` - fieldExpression to compare a range of values to the value in a datetime 
field. Each literal is a range of time beginning with midnight (00:00:00). For example: `YESTERDAY`, `LAST_WEEK`, 
`LAST_MONTH` ...

**Duration:** SObject query duration filter is applied to system field `LastModifiedDate` to allow range query. 
Duration units set to `hours`. For example, if duration is '6' (6 hours) and the pipeline runs at 9am, it will read data 
updated from 3am - 9am. Ignored if `datetimeFilter` is provided.

**Offset:** SObject query offset filter is applied to system field `LastModifiedDate` to allow range query. 
Offset units set to `hours`. For example, if duration is '6' (6 hours) and the offset is '1' (1 hour) and the pipeline 
runs at 9am, it will read data updated from 2am - 8am. Ignored if `datetimeFilter` is provided.

**Error Handling:** Strategy used to handle erroneous records. Acceptable values are Skip on error,
Send to error, Stop on error.


    +---------------+----------------------------------------------------------------------------------------------------+
    |     Value     |                                            Description                                             |
    +---------------+----------------------------------------------------------------------------------------------------+
    | Skip on error | Ignores erroneous records                                                                          |
    | Send to error | Emits an error to error handler. Errors are records with a field 'body', containing erroneous row. |
    | Stop on error | Fails pipeline due to erroneous record                                                             |
    +---------------+----------------------------------------------------------------------------------------------------+
    

**SObject Name Field**: The name of the field that holds the SObject name. 
Must not be the name of any SObject column that will be read. Defaults to `tablename`.
    
Example
----------

There are two SObjects of interest in Salesforce.
The first SObject is named 'Account' and contains:

    +-----+----------+------------------+
    | Id  | Name     | Email            |
    +-----+----------+------------------+
    | 0   | Samuel   | sjax@example.net |
    | 1   | Alice    | a@example.net    |
    +-----+----------+------------------+

The second is named 'Activity' and contains:

    +--------+----------+--------+
    | Id     | Item     | Action |
    +--------+----------+--------+
    | 0      | shirt123 | view   |
    | 0      | carxyz   | view   |
    | 0      | shirt123 | buy    |
    | 0      | coffee   | view   |
    | 1      | cola     | buy    |
    +--------+----------+--------+
    
To read data from these two SObjects, both of them must be indicated in the `White List` configuration property.

The output of the the source will be the following records:

    +-----+----------+------------------+-----------+
    | Id  | Name     | Email            | tablename |
    +-----+----------+------------------+-----------+
    | 0   | Samuel   | sjax@example.net | Account   |
    | 1   | Alice    | a@example.net    | Account   |
    +-----+----------+------------------+-----------+
    +--------+----------+--------+-----------+
    | Id     | Item     | Action | tablename |
    +--------+----------+--------+-----------+
    | 0      | shirt123 | view   | Activity  |
    | 0      | carxyz   | view   | Activity  |
    | 0      | shirt123 | buy    | Activity  |
    | 0      | coffee   | view   | Activity  |
    | 1      | cola     | buy    | Activity  |
    +--------+----------+--------+-----------+
    
The plugin will emit two pipeline arguments to provide multi sink plugin with the schema of the output records:

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
     