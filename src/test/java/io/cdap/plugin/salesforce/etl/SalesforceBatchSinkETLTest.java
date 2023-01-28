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
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;

import org.junit.Assert;
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
  public void testUpdateAccount() throws Exception {
    String sObject = "Account";
    Schema schema = Schema.recordOf("output",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ShippingLatitude", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("ShippingLongitude", Schema.of(Schema.Type.DOUBLE))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", "testUpdateAccount1")
        .set("NumberOfEmployees", 6)
        .set("ShippingLatitude", 50.4501)
        .set("ShippingLongitude", 30.5234)
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testUpdateAccount2")
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

    schema = Schema.recordOf("output",
      Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ShippingLatitude", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("ShippingLongitude", Schema.of(Schema.Type.DOUBLE))
    );

    inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Id", createdObjectsIds.get(0))
        .set("Name", "testUpdateAccount1Stage2")
        .set("NumberOfEmployees", 6)
        .set("ShippingLatitude", 50.4501)
        .set("ShippingLongitude", 30.5234)
        .build(),
      StructuredRecord.builder(schema)
        .set("Id", createdObjectsIds.get(1))
        .set("Name", "testUpdateAccount2Stage2")
        .set("NumberOfEmployees", 1)
        .set("ShippingLatitude", 37.4220)
        .set("ShippingLongitude", 122.0841)
        .build()
    );

    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
      .put(SalesforceSinkConfig.PROPERTY_OPERATION, "Update")
      .build();

    appManager = deployPipeline(sObject, schema, sinkProperties);
    runPipeline(appManager, inputRecords);
    assertRecordsCreated(sObject, inputRecords, createdSObjects);
    addToCleanUpList(createdSObjects);
  }

  @Test
  public void testUpsertOpportunity() throws Exception {
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

    List<StructuredRecord> inputRecords1 = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", "testUpsertOpportunity1")
        .set("StageName", "Prospecting")
        .set("CloseDate", 17897).set("IsPrivate", true)
        .set("Amount", 123.0)
        .set("ForecastCategoryName", "Omitted")
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testUpsertOpportunity2")
        .set("StageName", "Closed Won")
        .set("CloseDate", 17897)
        .set("IsPrivate", false)
        .set("Amount", 0.0)
        .set("ForecastCategoryName", "Closed")
        .build()
    );
    List<SObject> createdSObjects = new ArrayList<>();

    ApplicationManager appManager = deployPipeline(sObject, schema);
    runPipeline(appManager, inputRecords1);
    assertRecordsCreated(sObject, inputRecords1, createdSObjects);
    addToCleanUpList(createdSObjects);

    schema = Schema.recordOf("output",
      Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("IsPrivate", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("Amount", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("ForecastCategoryName", Schema.of(Schema.Type.STRING))
    );

    List<StructuredRecord> inputRecords2 = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Id", createdObjectsIds.get(0))
        .set("Name", "testUpsertOpportunity1Stage2")
        .set("StageName", "Prospecting")
        .set("CloseDate", 17897)
        .set("IsPrivate", true)
        .set("Amount", 123.0)
        .set("ForecastCategoryName", "Omitted")
        .build(),
      StructuredRecord.builder(schema)
        .set("Id", "")
        .set("Name", "testUpsertOpportunity2Stage2")
        .set("StageName", "Closed Won")
        .set("CloseDate", 17897)
        .set("IsPrivate", false)
        .set("Amount", 0.0)
        .set("ForecastCategoryName", "Closed")
        .build()
    );
    createdSObjects = new ArrayList<>();

    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
      .put(SalesforceSinkConfig.PROPERTY_OPERATION, "Upsert")
      .put(SalesforceSinkConfig.PROPERTY_EXTERNAL_ID_FIELD, "Id")
      .build();

    appManager = deployPipeline(sObject, schema, sinkProperties);
    runPipeline(appManager, inputRecords2);

    List<StructuredRecord> expectedRecords = new ArrayList<>();
    expectedRecords.add(inputRecords1.get(1));
    expectedRecords.addAll(inputRecords2);

    assertRecordsCreated(sObject, expectedRecords, createdSObjects);
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
      StructuredRecord.builder(schema)
        .set("NaMe", "testInsertAccount1")
        .set("numberofEmployEES", 6)
        .set("ShippingLatitudE", 50.4501)
        .set("shippingLongitude", 30.5234)
        .build(),
      StructuredRecord.builder(schema)
        .set("NaMe", "testInsertAccount2")
        .set("numberofEmployEES", 1)
        .set("ShippingLatitudE", 37.4220)
        .set("shippingLongitude", 122.0841)
        .build()
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

    ApplicationManager appManager = deployPipeline(sObject, schema, sinkProperties);
    runPipeline(appManager, inputRecords);
    assertRecordsCreated(sObject, inputRecords, createdSObjects);
    addToCleanUpList(createdSObjects);
  }

  @Test
  public void testCompoundFieldsValidation() {
    String sObject = "Account";
    Schema schema = Schema.recordOf("output",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
      // no compund fields like 'BillingAddress' are allowed.
      // Schema validation should fail
      Schema.Field.of("BillingAddress", Schema.of(Schema.Type.STRING))
    );

    MockFailureCollector collector = new MockFailureCollector();
    OAuthInfo oAuthInfo =
      SalesforceConnectionUtil.getOAuthInfo(
        getDefaultConfig(sObject).getConnection().getAuthenticatorCredentials(), collector);
    getDefaultConfig(sObject).validate(schema, collector, oAuthInfo);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testNonCreatableFieldsValidation() {
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

    MockFailureCollector collector = new MockFailureCollector();
    OAuthInfo oAuthInfo =
      SalesforceConnectionUtil.getOAuthInfo(
        getDefaultConfig(sObject).getConnection().getAuthenticatorCredentials(), collector);
    getDefaultConfig(sObject).validate(schema, collector, oAuthInfo);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testNonExistingFieldsValidation() {
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

    MockFailureCollector collector = new MockFailureCollector();
    OAuthInfo oAuthInfo =
      SalesforceConnectionUtil.getOAuthInfo(
        getDefaultConfig(sObject).getConnection().getAuthenticatorCredentials(), collector);
    getDefaultConfig(sObject).validate(schema, collector, oAuthInfo);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }
}
