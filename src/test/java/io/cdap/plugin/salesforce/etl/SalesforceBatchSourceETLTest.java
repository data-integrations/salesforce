/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.salesforce.etl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sforce.soap.metadata.CustomField;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.sobject.SObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * {@inheritDoc}
 */
public class SalesforceBatchSourceETLTest extends BaseSalesforceBatchSourceETLTest {

  @Test
  public void testTypesConversion() throws Exception {
    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Opportunity")
             .put("Name", "testTypesConversion1")
             .put("Probability", "50")
             .put("Amount", "25000")
             .put("StageName", "Proposal")
             .put("CloseDate", Date.from(Instant.now()))
             .put("TotalOpportunityQuantity", "25")
             .build())
      .build();

    addSObjects(sObjects);

    String query = "SELECT Id, IsDeleted, Type, Probability, ExpectedRevenue, TotalOpportunityQuantity, " +
      "LastActivityDate, LastModifiedDate FROM Opportunity WHERE Name LIKE 'testTypesConversion%'";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);

    Assert.assertEquals(sObjects.size(), records.size());

    StructuredRecord record = records.get(0);
    Schema recordSchema = record.getSchema();

    Schema expectedSchema = Schema.recordOf("output",
                                            Schema.Field.of("Id",
                                                            Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("IsDeleted",
                                                            Schema.of(Schema.Type.BOOLEAN)),
                                            Schema.Field.of("Type",
                                                            Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("Probability",
                                                            Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("ExpectedRevenue",
                                                            Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("TotalOpportunityQuantity",
                                                            Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("LastActivityDate",
                                                            Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                            Schema.Field.of("LastModifiedDate",
                                                            Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
    );

    Assert.assertEquals(expectedSchema.toString(), recordSchema.toString());
  }

  @Test
  public void testValuesReturned() throws Exception {
    Instant now = OffsetDateTime.now(ZoneOffset.UTC).toInstant();

    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Opportunity")
             .put("Name", "testValuesReturned1")
             .put("Amount", "25000")
             .put("StageName", "Proposal")
             .put("CloseDate", Date.from(now))
             .put("TotalOpportunityQuantity", "25")
             .build())
      .build();

    addSObjects(sObjects);

    String query = "SELECT StageName, IsDeleted, Type, TotalOpportunityQuantity, CloseDate" +
      " FROM Opportunity WHERE Name LIKE 'testValuesReturned%'";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);

    Assert.assertEquals(sObjects.size(), records.size());

    StructuredRecord record = records.get(0);

    Assert.assertEquals("Proposal", record.get("StageName"));
    Assert.assertFalse((boolean) record.get("IsDeleted"));
    Assert.assertEquals(25.0, (double) record.get("TotalOpportunityQuantity"), 0.01);
    Assert.assertEquals(LocalDateTime.ofInstant(now, ZoneOffset.UTC).toLocalDate(), record.getDate("CloseDate"));
  }

  @Test
  public void testLikeClause() throws Exception {
    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Account")
             .put("Name", "testLikeClause1")
             .build())
      .add(new SObjectBuilder()
             .setType("Account")
             .put("Name", "testLikeClause2")
             .build())
      .add(new SObjectBuilder()
             .setType("Account")
             .put("Name", "testLikeClause3")
             .build())
      .build();

    addSObjects(sObjects);

    String query = "SELECT Id, Name FROM Account WHERE Name LIKE 'testLikeClause%'";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);

    Assert.assertEquals(sObjects.size(), records.size());

    Set<String> names = new HashSet<>();
    names.add(records.get(0).get("Name"));
    names.add(records.get(1).get("Name"));
    names.add(records.get(2).get("Name"));

    Set<String> expectedNames = new HashSet<>();
    expectedNames.add("testLikeClause1");
    expectedNames.add("testLikeClause2");
    expectedNames.add("testLikeClause3");
    Assert.assertEquals(expectedNames, names);
  }

  @Test
  public void testWhereClause() throws Exception {
    // Login entry will be created automatically no need to create it
    String query = "SELECT UserId from LoginHistory WHERE LoginTime > " +
      "2000-09-20T22:16:30.000Z AND LoginTime < 9999-09-21T22:16:30.000Z";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);
    Assert.assertNotEquals(0, records.size());

    StructuredRecord record = records.get(0);
    String userId = record.get("UserId");
    Assert.assertNotNull(userId);
    Assert.assertFalse(userId.isEmpty());
  }

  @Test
  public void testEmptyQueryResponse() throws Exception {
    String query = "SELECT Id from Opportunity LIMIT 0";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);
    Assert.assertEquals(0, records.size());
  }

  @Test
  public void testSOQLSyntaxError() throws Exception {
    String query = "SELECTS something FROM table";
    try {
      getResultsBySOQLQuery(query);
      Assert.fail("Expected syntax error");
    } catch (IllegalStateException e) {
      /*
      java.lang.IllegalStateException: Expected 200 OK, got 400 Bad Request. Error: Failed to deploy app


      This is exception from workflow manager, not the actual exception with syntax error.
      Seems like there are no methods implemented to get exception which failed the pipeline.
      */
    }
  }

  @Test
  public void testSObjectQuerySchema() throws Exception {
    CustomField customField = createTextCustomField("CustomField__c");

    String sObjectName = createCustomObject("IT_SObjectSchema", new CustomField[]{customField});

    SObject sObject = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Fred")
      .put("CustomField__c", "Custom")
      .build();

    addSObjects(Collections.singletonList(sObject), false);

    DescribeSObjectResult describeResult = partnerConnection.describeSObject(sObjectName);
    List<String> expectedFieldNames = Stream.of(describeResult.getFields())
      .map(Field::getName)
      .collect(Collectors.toList());

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, null, null, null);

    Assert.assertEquals(1, results.size());

    List<Schema.Field> fields = results.get(0).getSchema().getFields();
    Assert.assertNotNull(fields);

    List<String> actualFieldNames = fields.stream()
      .map(Schema.Field::getName)
      .collect(Collectors.toList());

    // field order should match
    Assert.assertEquals(expectedFieldNames, actualFieldNames);
  }

  @Test
  public void testSObjectWithSchema() throws Exception {
    String sObjectName = createCustomObject("IT_WithSchema", null);

    SObject sObject = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Fred")
      .build();

    addSObjects(Collections.singletonList(sObject), false);

    Schema providedSchema = Schema.recordOf("output",
      Schema.Field.of("Name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, null, null, providedSchema.toString());

    Assert.assertEquals(1, results.size());

    Schema actualSchema = results.get(0).getSchema();
    Assert.assertEquals(providedSchema.toString(), actualSchema.toString());
  }

  @Test
  public void testSObjectQueryNoFilters() throws Exception {
    String sObjectName = createCustomObject("IT_NoFilters", null);
    String nameField = "Name";
    Set<String> names = ImmutableSet.of("Fred", "Wilma", "Pebbles", "Dino");

    List<SObject> sObjects = names.stream()
      .map(name -> new SObjectBuilder().setType(sObjectName).put(nameField, name).build())
      .collect(Collectors.toList());

    addSObjects(sObjects, false);

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, null, null, null);

    Assert.assertEquals(names.size(), results.size());

    Set<String> actualNames = results.stream()
      .map(record -> (String) record.get(nameField))
      .collect(Collectors.toSet());

    Assert.assertEquals(names, actualNames);
  }

  @Test
  public void testSObjectQueryWithIncrementalFilter() throws Exception {
    String sObjectName = createCustomObject("IT_IncFilter", null);

    SObject sObject1 = new SObjectBuilder()
        .setType(sObjectName)
        .put("Name", "Fred")
        .build();

    addSObjects(Collections.singletonList(sObject1), false);

    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    String datetimeAfter = now.plusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);
    String datetimeBefore = now.plusHours(1).format(DateTimeFormatter.ISO_DATE_TIME);

    // we can not modify LastModifiedDate, add new record after 2 seconds to be able to check filtering
    TimeUnit.SECONDS.sleep(2);

    SObject sObject2 = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Wilma")
      .build();

    addSObjects(Collections.singletonList(sObject2), false);

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, datetimeAfter, datetimeBefore, null);

    Assert.assertEquals(1, results.size());
    Assert.assertEquals("Wilma", results.get(0).get("Name"));
  }

  @Test
  public void testWideObjectQuery() throws Exception {
    String nameTemplate = "%s__c";
    CustomField[] customFields = IntStream.range(0, SalesforceConstants.SOQL_MAX_LENGTH / MAX_FIELD_NAME_LENGTH)
      // generate field name like FFF..FF1__c
      .mapToObj(i -> String.format(nameTemplate, Strings.padStart(String.valueOf(i), MAX_FIELD_NAME_LENGTH, 'F')))
      .map(this::createTextCustomField)
      .toArray(CustomField[]::new);

    String sObjectName = createCustomObject("IT_WideObjectQuery", customFields);

    String nameField = "Name";
    SObject sObject = new SObjectBuilder()
      .setType(sObjectName)
      .put(nameField, "Barney")
      .build();

    addSObjects(Collections.singletonList(sObject), false);

    List<String> customFieldNames = Stream.of(customFields)
      .map(CustomField::getFullName)
      .collect(Collectors.toList());

    List<String> requestedFields = ImmutableList.<String>builder()
      .add(nameField)
      .addAll(customFieldNames)
      .build();

    String query = String.format("SELECT %s FROM %s", String.join(",", requestedFields), sObjectName);
    Assert.assertFalse(SalesforceQueryUtil.isQueryUnderLengthLimit(query));

    List<StructuredRecord> results = getResultsBySOQLQuery(query);

    Assert.assertEquals(1, results.size());

    StructuredRecord record = results.get(0);
    List<Schema.Field> schemaFields = record.getSchema().getFields();

    Assert.assertNotNull(schemaFields);
    Assert.assertEquals(requestedFields.size(), schemaFields.size());
    Assert.assertEquals(sObject.getField(nameField), record.get(nameField));

    requestedFields.forEach(fieldName -> Assert.assertNotNull(
      String.format("Field '%s' not found", fieldName), record.getSchema().getField(fieldName)));
  }

  @Test
  public void testSObjectQueryWithCompoundField() throws Exception {
    CustomField customField = createLocationCustomField("CustomField__c");

    String sObjectName = createCustomObject("IT_CompoundField", new CustomField[]{customField});

    SObject sObject = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Bamm-Bamm")
      // CustomField__c can be set through individual fields
      .put("CustomField__Latitude__s", 3.33) // location individual field Latitude
      .put("CustomField__Longitude__s", 55.779) // location individual field Longitude
      .build();

    addSObjects(Collections.singletonList(sObject), false);

    DescribeSObjectResult describeResult = partnerConnection.describeSObject(sObjectName);
    List<String> expectedFieldNames = Stream.of(describeResult.getFields())
      .map(Field::getName)
      .filter(name -> !customField.getFullName().equals(name)) // exclude compound field name
      .collect(Collectors.toList());

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, null, null, null);

    Assert.assertEquals(1, results.size());

    List<Schema.Field> fields = results.get(0).getSchema().getFields();
    Assert.assertNotNull(fields);

    List<String> actualFieldNames = fields.stream()
      .map(Schema.Field::getName)
      .collect(Collectors.toList());

    // field order should match
    Assert.assertEquals(expectedFieldNames, actualFieldNames);
    Assert.assertEquals("Bamm-Bamm", results.get(0).get("Name"));
    Assert.assertEquals(3.33, (double) results.get(0).get("CustomField__Latitude__s"), 0.0);
    Assert.assertEquals(55.779, (double) results.get(0).get("CustomField__Longitude__s"), 0.0);
  }

  @Test
  public void testSOQLAggregateFunction() throws Exception {
    String sObjectName = createCustomObject("IT_AggFunctions", null);

    int numberOfRecords = 3;
    List<SObject> sObjects = IntStream.range(0, numberOfRecords)
      .mapToObj(i -> new SObjectBuilder()
        .setType(sObjectName)
        .put("Name", "R-" + i)
        .build())
      .collect(Collectors.toList());

    addSObjects(sObjects, false);

    List<StructuredRecord> results = getResultsBySOQLQuery("SELECT COUNT(name) cnt FROM " + sObjectName);

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(numberOfRecords, (long) results.get(0).get("cnt"));
  }

  @Test
  public void testSOQLGroupByClause() throws Exception {
    CustomField textField = createTextCustomField("TextField__c");
    CustomField locationField = createLocationCustomField("LocationField__c");
    String sObjectName = createCustomObject("IT_GroupByClause", new CustomField[]{textField, locationField});

    int numberOfRecords = 3;
    List<SObject> sObjects = IntStream.range(0, numberOfRecords)
      .mapToObj(i -> new SObjectBuilder()
        .setType(sObjectName)
        .put("Name", "Dino")
        .put("TextField__c", String.valueOf(i))
        .put("LocationField__Latitude__s", i)
        .put("LocationField__Longitude__s", i)
        .build())
      .collect(Collectors.toList());

    addSObjects(sObjects, false);

    List<StructuredRecord> results = getResultsBySOQLQuery(
      String.format("SELECT Name, MAX(TextField__c) maxDesc, AVG(LocationField__Latitude__s) avgLat, "
                      + "COUNT(TextField__c) cnt, SUM(LocationField__Longitude__s) sumLon "
                      + "FROM %s "
                      + "GROUP BY Name", sObjectName));

    Assert.assertEquals(1, results.size());
    Assert.assertEquals("Dino", results.get(0).get("Name"));
    Assert.assertEquals("2", results.get(0).get("maxDesc"));
    Assert.assertEquals(1.0, (double) results.get(0).get("avgLat"), 0.0);
    Assert.assertEquals(3.0, (double) results.get(0).get("sumLon"), 0.0);
    Assert.assertEquals(numberOfRecords, (long) results.get(0).get("cnt"));
  }

  @Test
  public void testSOQLReferenceField() throws Exception {
    String childSObjectName = createCustomObject("IT_ChildObject", null);
    SObject childSObject = new SObjectBuilder()
      .setType(childSObjectName)
      .put("Name", "Bamm-Bamm")
      .build();

    List<String> childIds = addSObjects(Collections.singletonList(childSObject), false);

    Assert.assertEquals(1, childIds.size());
    String childId = childIds.get(0);

    CustomField customField = createReferenceCustomField(childSObjectName);
    String sObjectName = createCustomObject("IT_ReferenceField", new CustomField[]{customField});

    SObject sObjectWilma = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Wilma")
      .put(childSObjectName, childId)
      .build();

    SObject sObjectFred = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Fred")
      .put(childSObjectName, childId)
      .build();

    addSObjects(Arrays.asList(sObjectWilma, sObjectFred), false);

    List<StructuredRecord> results = getResultsBySOQLQuery(
      String.format("SELECT COUNT(%s__r.Name) cnt FROM %s", customField.getLabel(), sObjectName));

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(2, (long) results.get(0).get("cnt"));
  }

  @Test
  public void testSOQLSubQuery() throws Exception {
    String childSObjectName = createCustomObject("IT_ChildObject", null);
    SObject childSObject = new SObjectBuilder()
      .setType(childSObjectName)
      .put("Name", "Bamm-Bamm")
      .build();

    List<String> childIds = addSObjects(Collections.singletonList(childSObject), false);

    Assert.assertEquals(1, childIds.size());
    String childId = childIds.get(0);

    CustomField customField = createReferenceCustomField(childSObjectName);
    String sObjectName = createCustomObject("IT_ReferenceField", new CustomField[]{customField});

    SObject sObjectWilma = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Wilma")
      .put(childSObjectName, childId)
      .build();

    SObject sObjectFred = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Fred")
      .put(childSObjectName, childId)
      .build();

    addSObjects(Arrays.asList(sObjectWilma, sObjectFred), false);

    String referencePostfix = "__r";
    List<StructuredRecord> results = getResultsBySOQLQuery(
      String.format("SELECT Id, (select Name from %s%s) FROM %s",
                    customField.getRelationshipName(), referencePostfix, childSObjectName));

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(childId, results.get(0).get("Id"));

    List<StructuredRecord> subQueryResults = results.get(0).get(customField.getRelationshipName() + referencePostfix);
    Assert.assertNotNull(subQueryResults);
    Assert.assertEquals(2, subQueryResults.size());

    subQueryResults.sort(Comparator.comparing(record -> record.get("Name")));
    Assert.assertEquals("Fred", subQueryResults.get(0).get("Name"));
    Assert.assertEquals("Wilma", subQueryResults.get(1).get("Name"));
  }

  @Test
  public void testPKChunkEnable() throws Exception {
    ImmutableMap<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("enablePKChunk", "true")
      .put("chunkSize", "2")
      .build();
    testPKChunk(properties);
  }

  @Test
  public void testPKChunkEnableWithDefaultChunkSize() throws Exception {
    ImmutableMap<String, String> properties = new ImmutableMap.Builder<String, String>().put("enablePKChunk", "true")
      .build();
    testPKChunk(properties);
  }

  private void testPKChunk(ImmutableMap<String, String> properties) throws Exception {
    String sObjectName = createCustomObject("IT_PKChunk", null);

    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType(sObjectName)
             .put("Name", "record1")
             .build())
      .add(new SObjectBuilder()
             .setType(sObjectName)
             .put("Name", "record2")
             .build())
      .add(new SObjectBuilder()
             .setType(sObjectName)
             .put("Name", "record3")
             .build())
      .add(new SObjectBuilder()
             .setType(sObjectName)
             .put("Name", "record4")
             .build())
      .build();

    addSObjects(sObjects, true);


    ImmutableMap.Builder<String, String> baseProperties = getBaseProperties("SalesforceRederPKChunk");
    baseProperties.putAll(properties);
    baseProperties.put(SalesforceSourceConstants.PROPERTY_QUERY, "Select Name from " + sObjectName);

    List<StructuredRecord> results = getPipelineResults(baseProperties.build(), SalesforceBatchSource.NAME,
                                                        "SalesforceBatch");
    results.sort(Comparator.comparing(record -> record.get("Name")));
    Assert.assertEquals(4, results.size());
    Assert.assertEquals("record1", results.get(0).get("Name"));
    Assert.assertEquals("record2", results.get(1).get("Name"));
    Assert.assertEquals("record3", results.get(2).get("Name"));
    Assert.assertEquals("record4", results.get(3).get("Name"));
  }
}
