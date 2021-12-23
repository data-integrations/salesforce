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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sforce.soap.partner.sobject.SObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.test.ProgramManager;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@inheritDoc}
 */
public class SalesforceStreamingSourceETLTest extends BaseSalesforceStreamingSourceETLTest {

  @Test
  public void testTypesConversion() throws Exception {
    String query = "SELECT Id, IsDeleted, Type, Probability, ExpectedRevenue, TotalOpportunityQuantity, " +
      "LastActivityDate, LastModifiedDate FROM Opportunity";

    ProgramManager programManager = startPipeline(ImmutableMap.<String, String>builder()
                                                    .put("pushTopicQuery", query)
                                                    .build());

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

    List<StructuredRecord> records = waitForRecords(programManager, sObjects.size());

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

    Assert.assertEquals(expectedSchema, recordSchema);
  }

  @Test
  public void testValuesReturned() throws Exception {
    String query = "SELECT Id, StageName, IsDeleted, Type, TotalOpportunityQuantity, CloseDate FROM Opportunity";
    ProgramManager programManager = startPipeline(ImmutableMap.<String, String>builder()
      .put("pushTopicQuery", query)
      .build());

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

    List<StructuredRecord> records = waitForRecords(programManager, sObjects.size());

    Assert.assertEquals(sObjects.size(), records.size());

    StructuredRecord record = records.get(0);

    Assert.assertEquals("Proposal", record.get("StageName"));
    Assert.assertFalse((boolean) record.get("IsDeleted"));
    Assert.assertEquals(25.0, (double) record.get("TotalOpportunityQuantity"), 0.01);
    Assert.assertEquals(LocalDateTime.ofInstant(now, ZoneOffset.UTC).toLocalDate(), record.getDate("CloseDate"));
  }

  @Test
  public void testFieldsCase() throws Exception {
    String query = "SELECT Id, Name FROM Account";
    ProgramManager programManager = startPipeline(ImmutableMap.<String, String>builder()
                                                    .put("pushTopicQuery", query)
                                                    .build());

    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Account")
             .put("NaMe", "testFieldsCase1")
             .build())
      .add(new SObjectBuilder()
             .setType("Account")
             .put("name", "testFieldsCase2")
             .build())
      .add(new SObjectBuilder()
             .setType("Account")
             .put("Name", "testFieldsCase3")
             .build())
      .build();

    addSObjects(sObjects);

    List<StructuredRecord> records = waitForRecords(programManager, sObjects.size());

    Assert.assertEquals(sObjects.size(), records.size());

    Set<String> names = new HashSet<>();
    for (StructuredRecord record : records) {
      names.add(record.get(record.getSchema().getField("name", true).getName()));
    }

    Set<String> expectedNames = new HashSet<>();
    expectedNames.add("testFieldsCase1");
    expectedNames.add("testFieldsCase2");
    expectedNames.add("testFieldsCase3");
    Assert.assertEquals(expectedNames, names);
  }

  @Test
  public void testQueryBySObjectName() throws Exception {
    ProgramManager programManager = startPipeline(ImmutableMap.<String, String>builder()
                                                    .put("sObjectName", "Opportunity")
                                                    .build());

    Instant now = OffsetDateTime.now(ZoneOffset.UTC).toInstant();

    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Opportunity")
             .put("Name", "testQueryBySObjectName1")
             .put("Amount", "25000")
             .put("StageName", "Proposal")
             .put("CloseDate", Date.from(now))
             .put("TotalOpportunityQuantity", "25")
             .build())
      .build();

    addSObjects(sObjects);

    List<StructuredRecord> records = waitForRecords(programManager, sObjects.size());

    Assert.assertEquals(sObjects.size(), records.size());

    StructuredRecord record = records.get(0);

    Assert.assertEquals("Proposal", record.get("StageName"));
    Assert.assertFalse((boolean) record.get("IsDeleted"));
    Assert.assertEquals(25.0, (double) record.get("TotalOpportunityQuantity"), 0.01);
    Assert.assertEquals(LocalDateTime.ofInstant(now, ZoneOffset.UTC).toLocalDate(), record.getDate("CloseDate"));
  }

  @Test
  public void testSOQLSyntaxError() throws Exception {
    String query = "SELECTS something FROM table";
    try {
      ProgramManager programManager = startPipeline(ImmutableMap.<String, String>builder()
                                                      .put("pushTopicQuery", query)
                                                      .build());
      Assert.fail("Expected syntax error");
    } catch (IllegalStateException e) {
      /*
      java.lang.IllegalStateException: Expected 200 OK, got 400 Bad Request. Error: Failed to deploy app


      This is exception from workflow manager, not the actual exception with syntax error.
      Seems like there are no methods implemented to get exception which failed the pipeline.
      */
    }
  }
}
