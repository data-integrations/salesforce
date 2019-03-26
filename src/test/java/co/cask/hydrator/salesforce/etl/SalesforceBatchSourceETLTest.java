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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


/**
 * {@inheritDoc}
 */
public class SalesforceBatchSourceETLTest extends BaseSalesforceBatchSourceETLTest {
  @Test
  public void testTypesConversion() throws Exception {
    String query = "SELECT Id, IsDeleted, Type, Probability, ExpectedRevenue, TotalOpportunityQuantity, " +
      "LastActivityDate, LastModifiedDate FROM Opportunity";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);
    Assert.assertNotEquals(0, records.size());

    StructuredRecord record = records.get(0);

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

    Assert.assertEquals(expectedSchema, record.getSchema());
  }

  @Test
  public void testValuesReturned() throws Exception {
    String query = "SELECT OwnerId, Name, IsDeleted FROM Account"; // select only non-nillable fields
    List<StructuredRecord> records = getResultsBySOQLQuery(query);
    Assert.assertNotEquals(0, records.size());

    StructuredRecord record = records.get(0);
    Assert.assertFalse(((String) record.get("OwnerId")).isEmpty());
    Assert.assertFalse(((String) record.get("Name")).isEmpty());
    Assert.assertTrue(record.get("IsDeleted") instanceof Boolean);
  }

  @Test
  public void testLikeClause() throws Exception {
    String query = "SELECT Id, Name FROM Account WHERE Name LIKE '%'";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);
    Assert.assertNotEquals(0, records.size());

    StructuredRecord record = records.get(0);
    Assert.assertFalse(((String) record.get("Name")).isEmpty());
  }

  @Test
  public void testWhereClause() throws Exception {
    String query = "SELECT UserId from LoginHistory WHERE LoginTime > " +
      "2000-09-20T22:16:30.000Z AND LoginTime < 9999-09-21T22:16:30.000Z";
    List<StructuredRecord> records = getResultsBySOQLQuery(query);
    Assert.assertNotEquals(0, records.size());

    StructuredRecord record = records.get(0);
    Assert.assertFalse(((String) record.get("UserId")).isEmpty());
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
