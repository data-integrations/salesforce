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

package co.cask.hydrator.salesforce.etl;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.salesforce.soap.SObjectBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sforce.soap.metadata.CustomField;
import com.sforce.soap.metadata.FieldType;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.sobject.SObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
                                                            Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))
    );

    Assert.assertEquals(expectedSchema, recordSchema);
  }

  @Test
  public void testValuesReturned() throws Exception {
    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Opportunity")
             .put("Name", "testValuesReturned1")
             .put("Amount", "25000")
             .put("StageName", "Proposal")
             .put("CloseDate", Date.from(Instant.now()))
             .put("TotalOpportunityQuantity", "25")
             .build())
      .build();

    addSObjects(sObjects);

    String query = "SELECT StageName, IsDeleted, Type, TotalOpportunityQuantity, LastModifiedDate" +
      " FROM Opportunity WHERE Name LIKE 'testValuesReturned%'";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);

    Assert.assertEquals(sObjects.size(), records.size());

    StructuredRecord record = records.get(0);

    Assert.assertEquals("Proposal", record.get("StageName"));
    Assert.assertFalse((boolean) record.get("IsDeleted"));
    Assert.assertEquals(25.0, (double) record.get("TotalOpportunityQuantity"), 0.01);
    Assert.assertEquals(System.currentTimeMillis(), (long) record.get("LastModifiedDate"),
                        TimeUnit.MINUTES.toMillis(30));
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
    CustomField customField = new CustomField();
    customField.setFullName("CustomField__c");
    customField.setLabel("Custom Field Label");
    customField.setType(FieldType.Text);
    customField.setLength(50);
    customField.setRequired(true);

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

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, null);

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
  public void testSObjectQueryNoFilters() throws Exception {
    String sObjectName = createCustomObject("IT_NoFilters", null);
    String nameField = "Name";
    Set<String> names = ImmutableSet.of("Fred", "Wilma", "Pebbles", "Dino");

    List<SObject> sObjects = names.stream()
      .map(name -> new SObjectBuilder().setType(sObjectName).put(nameField, name).build())
      .collect(Collectors.toList());

    addSObjects(sObjects, false);

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, null);

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

    String dateTimeFilter = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);

    // we can not modify LastModifiedDate, add new record after 2 seconds to be able to check filtering
    TimeUnit.SECONDS.sleep(2);

    SObject sObject2 = new SObjectBuilder()
      .setType(sObjectName)
      .put("Name", "Wilma")
      .build();

    addSObjects(Collections.singletonList(sObject2), false);

    List<StructuredRecord> results = getResultsBySObjectQuery(sObjectName, dateTimeFilter);

    Assert.assertEquals(1, results.size());
    Assert.assertEquals("Wilma", results.get(0).get("Name"));
  }
}
