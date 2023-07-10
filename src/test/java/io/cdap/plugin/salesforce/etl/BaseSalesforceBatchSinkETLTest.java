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

import com.sforce.async.ConcurrencyMode;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceBatchSink;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;
import io.cdap.plugin.salesforce.plugin.sink.batch.StructuredRecordToCSVRecordTransformer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@inheritDoc}
 */
public abstract class BaseSalesforceBatchSinkETLTest extends BaseSalesforceETLTest {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static final String REFERENCE_NAME = "SalesforceBatchSink-output";
  private int deployCounter;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      SalesforceBatchSink.class
    );
  }

  @Before
  public void init() {
    deployCounter = 0;
  }

  /**
   * Assert that records provided all exist in Salesforce with the exact fields.
   * If not throw and exception.
   *
   * @param sObject sObject name
   * @param inputRecords list of records which are checked exist in Salesforce
   * @param createdSObjects list of sObjects returned by Salesforce
   * @throws ConnectionException occurs due to failure to connect to Salesforce API
   */
  protected void assertRecordsCreated(String sObject, List<StructuredRecord> inputRecords,
                                      List<SObject> createdSObjects) throws ConnectionException {
    for (StructuredRecord record : inputRecords) {
      String query = getQueryByRecord(sObject, record);
      SObject[] sObjectsArray = partnerConnection.query(query).getRecords();
      if (sObjectsArray.length == 0) {
        String errorMessage = String.format("Not found record='%s'. Query='%s'", record, query);
        errorCollector.addError(new Throwable(errorMessage));
      }
      createdSObjects.addAll(Arrays.asList(sObjectsArray));
    }
  }

  protected void addToCleanUpList(List<SObject> sObjects) {
    List<String> sObjectIds = sObjects.stream()
      .map(SObject::getId)
      .collect(Collectors.toList());

    createdObjectsIds.addAll(sObjectIds);
  }

  /**
   * Gets a query for an sObject that is the same as {@link StructuredRecord} provided.
   *
   * @param sObject sObject name
   * @param record structured record
   * @return SOQL query for the given structured record
   */
  protected String getQueryByRecord(String sObject, StructuredRecord record) {
    List<Schema.Field> fields = record.getSchema().getFields();

    if (fields == null) {
      throw new IllegalArgumentException("Sink schema must not be null");
    }

    fields = fields.stream()
      .filter((field) -> !field.getName().toLowerCase().equals("id"))
      .collect(Collectors.toList());

    StringBuilder sb = new StringBuilder("SELECT Id, "); // select id always, since we need it to delete objects.

    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      String fieldName = field.getName();

      sb.append(fieldName);
      if (i != fields.size() - 1) {
        sb.append(", ");
      }
    }

    sb.append(" FROM ");
    sb.append(sObject);
    sb.append(" WHERE ");

    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      boolean isString = field.getSchema().getType().equals(Schema.Type.STRING);

      String fieldName = field.getName();
      sb.append(fieldName);
      sb.append(" = ");

      if (isString) {
        sb.append("'");
      }
      String value = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(record.get(fieldName), field);
      if (value != null) {
        sb.append(value);
      }
      if (isString) {
        sb.append("'");
      }
      if (i != fields.size() - 1) {
        sb.append(" AND ");
      }
    }

    return sb.toString();
  }

  protected ApplicationManager deployPipeline(String sObject, Schema schema) throws Exception {
    return deployPipeline(sObject, schema, Collections.emptyMap());
  }

  protected ApplicationManager deployPipeline(String sObject, Schema schema, Map<String, String> pluginProperties)
    throws Exception {
    deployCounter++;

    Map<String, String> sinkProperties = new HashMap<>(getBaseProperties(REFERENCE_NAME)
                                                         .put(SalesforceSinkConfig.PROPERTY_ERROR_HANDLING,
                                                              "Fail on Error")
      .put(SalesforceSinkConfig.PROPERTY_SOBJECT, sObject)
      .put(SalesforceSinkConfig.PROPERTY_OPERATION, "Insert")
      .put(SalesforceSinkConfig.PROPERTY_MAX_BYTES_PER_BATCH, "10000000")
      .put(SalesforceSinkConfig.PROPERTY_MAX_RECORDS_PER_BATCH, "10000")
      .build());

    sinkProperties.putAll(pluginProperties);

    String inputDatasetName = "output-batchsourcetest_" + testName.getMethodName() + "_" + deployCounter;
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName, schema));

    ETLStage sink = new ETLStage(SalesforceBatchSink.PLUGIN_NAME,
                                 new ETLPlugin(SalesforceBatchSink.PLUGIN_NAME,
                                               BatchSink.PLUGIN_TYPE, sinkProperties, null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app(
      SalesforceBatchSink.PLUGIN_NAME + "_" + testName.getMethodName() + "_" + deployCounter);
    return deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));
  }

  protected void runPipeline(ApplicationManager appManager, List<StructuredRecord> input) throws Exception {
    String inputDatasetName = "output-batchsourcetest_" + testName.getMethodName() + "_" + deployCounter;
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED,  5, TimeUnit.MINUTES);
  }

  protected SalesforceSinkConfig getDefaultConfig(String sObject) {
    return new SalesforceSinkConfig(REFERENCE_NAME,
                                    BaseSalesforceETLTest.CONSUMER_KEY, BaseSalesforceETLTest.CONSUMER_SECRET,
                                    BaseSalesforceETLTest.USERNAME, BaseSalesforceETLTest.PASSWORD,
                                    BaseSalesforceETLTest.LOGIN_URL, 30000, sObject, "Insert", null,
                                    ConcurrencyMode.Parallel.name(), "1000000", "10000", "Fail on Error",
                                    BaseSalesforceETLTest.SECURITY_TOKEN,
                                    null, null);
  }
}
