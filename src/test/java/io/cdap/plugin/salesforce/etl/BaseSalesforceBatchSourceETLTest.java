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

import com.google.common.collect.ImmutableMap;
import com.sforce.soap.metadata.CustomField;
import com.sforce.soap.metadata.CustomObject;
import com.sforce.soap.metadata.DeploymentStatus;
import com.sforce.soap.metadata.FieldType;
import com.sforce.soap.metadata.Metadata;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.soap.metadata.SaveResult;
import com.sforce.soap.metadata.SharingModel;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
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
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * {@inheritDoc}
 */
public abstract class BaseSalesforceBatchSourceETLTest extends BaseSalesforceETLTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSalesforceBatchSourceETLTest.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  // Salesforce field name length limitation
  protected static final int MAX_FIELD_NAME_LENGTH = 40;
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static final String REFERENCE_NAME = "SalesforceBatchSource-input";
  private static final String METADATA_LOGIN_URL = "https://login.salesforce.com/services/Soap/u/45.0";

  private List<String> customObjects = new ArrayList<>();

  protected static MetadataConnection metadataConnection;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD, LOGIN_URL);
    } catch (AssumptionViolatedException e) {
      LOG.warn("ETL tests are skipped. Please find the instructions on enabling it at" +
                           "BaseSalesforceBatchSourceETLTest javadoc");
      throw e;
    }

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      SalesforceBatchSource.class,
                      SObject.class // should be loaded by Plugin ClassLoader to avoid SOAP deserialization issue
    );

    metadataConnection = createMetadataConnection();
  }

  @After
  public void cleanUp() throws ConnectionException {
    deleteCustomObjects();
  }

  /**
   * Creates custom object with provided list of custom fields.
   * Custom object full name will be saved in order to be deleted in the end of test run.
   *
   * @param baseName object base name (without `__c` suffix)
   * @param fields list of custom fields
   * @return object full name
   */
  protected String createCustomObject(String baseName, CustomField[] fields) throws ConnectionException {
    CustomObject customObject = initCustomObject(baseName, fields);
    String fullName = customObject.getFullName();
    customObjects.add(fullName);

    SaveResult[] results = metadataConnection.createMetadata(new Metadata[]{customObject});
    for (SaveResult result : results) {
      if (!result.isSuccess()) {
        String errors = Stream.of(result.getErrors())
          .map(com.sforce.soap.metadata.Error::getMessage)
          .collect(Collectors.joining("\n"));
        throw new RuntimeException("Failed to create custom object:\n" + errors);
      }
    }

    return fullName;
  }

  protected List<StructuredRecord> getResultsBySOQLQuery(String query) throws Exception {
    ImmutableMap.Builder<String, String> propsBuilder = getBaseProperties(REFERENCE_NAME)
      .put(SalesforceSourceConstants.PROPERTY_QUERY, query);

    return getPipelineResults(propsBuilder.build());
  }

  protected List<StructuredRecord> getResultsBySObjectQuery(String sObjectName,
                                                            String datetimeFilter,
                                                            String schema) throws Exception {
    ImmutableMap.Builder<String, String> propsBuilder = getBaseProperties(REFERENCE_NAME)
      .put(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME, sObjectName);

    if (datetimeFilter != null) {
      propsBuilder.put(SalesforceSourceConstants.PROPERTY_DATETIME_FILTER, datetimeFilter);
    }

    if (schema != null) {
      propsBuilder.put(SalesforceSourceConstants.PROPERTY_SCHEMA, schema);
    }

    return getPipelineResults(propsBuilder.build());
  }

  protected CustomField createTextCustomField(String fullName) {
    CustomField customField = new CustomField();
    customField.setFullName(fullName);
    // custom field name length can be 43 (max length + postfix `__c`)
    // substring field name to be within the label length limit
    customField.setLabel(fullName.substring(Math.max(0, fullName.length() - MAX_FIELD_NAME_LENGTH)));
    customField.setType(FieldType.Text);
    customField.setLength(50);
    customField.setRequired(true);
    customField.setDefaultValue("\"DefaultValue\"");
    return customField;
  }

  private static MetadataConnection createMetadataConnection() throws ConnectionException {

    ConnectorConfig loginConfig = new ConnectorConfig();
    loginConfig.setAuthEndpoint(METADATA_LOGIN_URL);
    loginConfig.setServiceEndpoint(METADATA_LOGIN_URL);
    loginConfig.setManualLogin(true);
    LoginResult loginResult = new PartnerConnection(loginConfig).login(USERNAME, PASSWORD);

    ConnectorConfig metadataConfig = new ConnectorConfig();
    metadataConfig.setServiceEndpoint(loginResult.getMetadataServerUrl());
    metadataConfig.setSessionId(loginResult.getSessionId());
    return new MetadataConnection(metadataConfig);
  }

  private CustomObject initCustomObject(String baseName, CustomField[] fields) {
    String fullName = baseName + "_" + System.currentTimeMillis() + "__c";
    CustomObject customObject = new CustomObject();
    customObject.setFullName(fullName);
    customObject.setLabel(fullName);
    customObject.setPluralLabel(fullName);
    customObject.setDeploymentStatus(DeploymentStatus.Deployed);
    customObject.setDescription("Created by the Metadata API for Integration Tests");
    customObject.setEnableActivities(true);
    customObject.setSharingModel(SharingModel.ReadWrite);

    CustomField nameField = new CustomField();
    nameField.setType(FieldType.Text);
    nameField.setLabel(customObject.getFullName() + " Name Field");
    customObject.setNameField(nameField);

    if (fields != null) {
      customObject.setFields(fields);
    }
    return customObject;
  }

  private void deleteCustomObjects() throws ConnectionException {
    if (customObjects.isEmpty()) {
      return;
    }

    String[] fullNames = customObjects.toArray(new String[0]);
    customObjects.clear();
    metadataConnection.deleteMetadata("CustomObject", fullNames);
  }

  private List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {
    ETLStage source = new ETLStage("SalesforceReader", new ETLPlugin("Salesforce",
                                                                     BatchSource.PLUGIN_TYPE,
                                                                     sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("SalesforceBatchSource_" + testName.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED,  5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }
}
