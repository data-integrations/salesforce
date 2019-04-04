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
import com.google.common.collect.ImmutableList;
import com.sforce.soap.partner.sobject.SObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
}
