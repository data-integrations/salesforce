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
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@inheritDoc}
 */
public class SalesforceBatchSinkETLTest extends BaseSalesforceBatchSinkETLTest {
  @Test
  public void testInsertAccount() throws Exception {
    String sObject = "Account";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("ShippingLatitude", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("ShippingLongitude", Schema.of(Schema.Type.DOUBLE))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", "testInsertAccount1")
        .set("NumberOfEmployees", 6)
        .set("ShippingLatitude", 50.4501)
        .set("ShippingLongitude", 30.5234)
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testInsertAccount2")
        .set("NumberOfEmployees", 1)
        .set("ShippingLatitude", 37.4220)
        .set("ShippingLongitude", 122.0841)
        .build()
    );
    List<SObject> createdSObjects = new ArrayList<>();

    ApplicationManager appManager = deployPipeline(sObject, schema);
    runPipeline(appManager, inputRecords);
    assertRecordsCreated(sObject, inputRecords, createdSObjects);
    addToCleanUpList(createdSObjects);
  }

  @Test
  public void testInsertOpportunity() throws Exception {
    String sObject = "Opportunity";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsPrivate", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("Amount", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("ForecastCategoryName", Schema.of(Schema.Type.STRING))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", "testInsertOpportunity1")
        .set("StageName", "Prospecting")
        .set("CloseDate", 17897).set("IsPrivate", true)
        .set("Amount", 123.0)
        .set("ForecastCategoryName", "Omitted")
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testInsertOpportunity2")
        .set("StageName", "Closed Won")
        .set("CloseDate", 17897)
        .set("IsPrivate", false)
        .set("Amount", 0.0)
        .set("ForecastCategoryName", "Closed")
        .build()
    );
    List<SObject> createdSObjects = new ArrayList<>();

    ApplicationManager appManager = deployPipeline(sObject, schema);
    runPipeline(appManager, inputRecords);
    assertRecordsCreated(sObject, inputRecords, createdSObjects);
    addToCleanUpList(createdSObjects);
  }

  @Test
  public void testNonDefaultFieldsCase() throws Exception {
    String sObject = "Account";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("NaMe", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("numberofEmployEES", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("ShippingLatitudE", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("shippingLongitude", Schema.of(Schema.Type.DOUBLE))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema).set("NaMe", "testInsertAccount1").set("numberofEmployEES", 6)
        .set("ShippingLatitudE", 50.4501).set("shippingLongitude", 30.5234).build(),
      StructuredRecord.builder(schema).set("NaMe", "testInsertAccount2").set("numberofEmployEES", 1)
        .set("ShippingLatitudE", 37.4220).set("shippingLongitude", 122.0841).build()
    );
    List<SObject> createdSObjects = new ArrayList<>();

    ApplicationManager appManager = deployPipeline(sObject, schema);
    runPipeline(appManager, inputRecords);
    assertRecordsCreated(sObject, inputRecords, createdSObjects);
    addToCleanUpList(createdSObjects);
  }

  @Test
  public void testMultipleBatches() throws Exception {
    String sObject = "Opportunity";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsPrivate", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("Amount", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("ForecastCategoryName", Schema.of(Schema.Type.STRING))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", "testMultipleBatches1")
        .set("StageName", "Prospecting")
        .set("CloseDate", 17897)
        .set("IsPrivate", true)
        .set("Amount", 123.0)
        .set("ForecastCategoryName", "Omitted")
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testMultipleBatches2")
        .set("StageName", "Closed Won")
        .set("CloseDate", 17897)
        .set("IsPrivate", false)
        .set("Amount", 5343.0)
        .set("ForecastCategoryName", "Closed")
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testMultipleBatches3")
        .set("StageName", "Needs Analysis")
        .set("CloseDate", 17897)
        .set("IsPrivate", false)
        .set("Amount", 11.0)
        .set("ForecastCategoryName", "Closed")
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testMultipleBatches4")
        .set("StageName", "Value Proposition")
        .set("CloseDate", 17897)
        .set("IsPrivate", true)
        .set("Amount", 88.0)
        .set("ForecastCategoryName", "Closed")
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testMultipleBatches5")
        .set("StageName", "Negotiation/Review")
        .set("CloseDate", 17897)
        .set("IsPrivate", false)
        .set("Amount", 0.0)
        .set("ForecastCategoryName", "Closed")
        .build()
    );

    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
      .put(SalesforceSinkConfig.PROPERTY_MAX_RECORDS_PER_BATCH, "1").build();
    List<SObject> createdSObjects = new ArrayList<>();

    ApplicationManager appManager = deployPipeline(sObject, schema);
    runPipeline(appManager, inputRecords);
    assertRecordsCreated(sObject, inputRecords, createdSObjects);
    addToCleanUpList(createdSObjects);
  }

  @Test(expected = IllegalStateException.class)
  public void testCompoundFieldsValidation() throws Exception {
    String sObject = "Account";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
                                    // no compund fields like 'BillingAddress' are allowed.
                                    // Schema validation should fail
                                    Schema.Field.of("BillingAddress", Schema.of(Schema.Type.STRING))
    );

    // Exception below is expected:
    // "java.lang.IllegalStateException: Expected 200 OK, got 400 Bad Request. Error: Failed to deploy app"
    // This indicates that validation (not actual execution) failed.
    ApplicationManager appManager = deployPipeline(sObject, schema);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonCreatableFieldsValidation() throws Exception {
    String sObject = "Opportunity";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsPrivate", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("Amount", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("ForecastCategoryName", Schema.of(Schema.Type.STRING)),
                                    // HasOverdueTask fields is present, but not-creatable in Opportunity sObject
                                    Schema.Field.of("HasOverdueTask", Schema.of(Schema.Type.STRING))
    );

    // Exception below is expected:
    // "java.lang.IllegalStateException: Expected 200 OK, got 400 Bad Request. Error: Failed to deploy app"
    // This indicates that validation (not actual execution) failed.
    deployPipeline(sObject, schema);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonExistingFieldsValidation() throws Exception {
    String sObject = "Opportunity";
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsPrivate", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("Amount", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("ForecastCategoryName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("SomethingNotExistant", Schema.of(Schema.Type.STRING))
    );

    // Exception below is expected:
    // "java.lang.IllegalStateException: Expected 200 OK, got 400 Bad Request. Error: Failed to deploy app"
    // This indicates that validation (not actual execution) failed.
    deployPipeline(sObject, schema);
  }
}
