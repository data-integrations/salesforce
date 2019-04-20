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
import com.sforce.soap.metadata.SharingModel;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
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
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
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
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.ErrorHandling;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Methods to run ETL with Salesforce Bulk plugin as source, and a mock plugin as a sink.
 *
 * By default all tests will be skipped, since Salesforce credentials are needed.
 *
 * Instructions to enable the tests:
 * 1. Create/use existing Salesforce account
 * 2. Create connected application within the account to get clientId and clientSecret
 * 3. Run the tests using the command below:
 *
 * mvn clean test
 * -Dsalesforce.test.clientId= -Dsalesforce.test.clientSecret= -Dsalesforce.test.username= -Dsalesforce.test.password=
 *
 */
public abstract class BaseSalesforceBatchSourceETLTest extends HydratorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSalesforceBatchSourceETLTest.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Rule
  public TestName name = new TestName();

  // Salesforce field name length limitation
  protected static final int MAX_FIELD_NAME_LENGTH = 40;
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static final String CLIENT_ID = System.getProperty("salesforce.test.clientId");
  private static final String CLIENT_SECRET = System.getProperty("salesforce.test.clientSecret");
  private static final String USERNAME = System.getProperty("salesforce.test.username");
  private static final String PASSWORD = System.getProperty("salesforce.test.password");
  private static final String LOGIN_URL = System.getProperty("salesforce.test.loginUrl",
                                                             "https://login.salesforce.com/services/oauth2/token");

  private static final String METADATA_LOGIN_URL = "https://login.salesforce.com/services/Soap/u/45.0";

  private List<SaveResult> createdObjectsIds = new ArrayList<>();
  private List<String> customObjects = new ArrayList<>();

  protected static PartnerConnection partnerConnection;
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

    AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(
      USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, LOGIN_URL);

    partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
    metadataConnection = createMetadataConnection();
  }

  @After
  public void cleanUp() throws ConnectionException {
    clearSObjects();
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

    com.sforce.soap.metadata.SaveResult[] results = metadataConnection.createMetadata(new Metadata[]{customObject});
    for (com.sforce.soap.metadata.SaveResult result : results) {
      if (!result.isSuccess()) {
        String errors = Stream.of(result.getErrors())
          .map(com.sforce.soap.metadata.Error::getMessage)
          .collect(Collectors.joining("\n"));
        throw new RuntimeException("Failed to create custom object:\n" + errors);
      }
    }

    return fullName;
  }

  /**
   * Creates given sObjects and saves their IDs for deletion in the end of test run.
   *
   * @param sObjects list of sObjects to be created
   */
  protected void addSObjects(List<SObject> sObjects) {
    addSObjects(sObjects, true);
  }

  /**
   * Adds sObjects to Salesforce. Checks the result response for errors.
   * If save flag is true, saves the objects so that they can be deleted after method is run.
   *
   * @param sObjects list of sobjects to create
   * @param save if sObjects need to be saved for deletion
   */
  protected void addSObjects(List<SObject> sObjects, boolean save) {
    try {
      SaveResult[] results = partnerConnection.create(sObjects.toArray(new SObject[0]));
      if (save) {
        createdObjectsIds.addAll(Arrays.asList(results));
      }

      for (SaveResult saveResult : results) {
        if (!saveResult.getSuccess()) {
          String allErrors = Stream.of(saveResult.getErrors())
            .map(Error::getMessage)
            .collect(Collectors.joining("\n"));

          throw new RuntimeException(allErrors);
        }
      }

    } catch (ConnectionException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  protected List<StructuredRecord> getResultsBySOQLQuery(String query) throws Exception {
    ImmutableMap.Builder<String, String> propsBuilder = getBaseProperties()
      .put(SalesforceSourceConstants.PROPERTY_QUERY, query);

    return getPipelineResults(propsBuilder.build());
  }

  protected List<StructuredRecord> getResultsBySObjectQuery(String sObjectName,
                                                            String datetimeFilter,
                                                            String schema) throws Exception {
    ImmutableMap.Builder<String, String> propsBuilder = getBaseProperties()
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


  private void clearSObjects() throws ConnectionException {
    if (createdObjectsIds.isEmpty()) {
      return;
    }

    String[] ids = createdObjectsIds.stream()
      .map(SaveResult::getId)
      .toArray(String[]::new);
    createdObjectsIds.clear();
    partnerConnection.delete(ids);
  }

  private void deleteCustomObjects() throws ConnectionException {
    if (customObjects.isEmpty()) {
      return;
    }

    String[] fullNames = customObjects.toArray(new String[0]);
    customObjects.clear();
    metadataConnection.deleteMetadata("CustomObject", fullNames);
  }

  private ImmutableMap.Builder<String, String> getBaseProperties() {
    return new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "SalesforceBulk-input")
      .put(SalesforceConstants.PROPERTY_CLIENT_ID, CLIENT_ID)
      .put(SalesforceConstants.PROPERTY_CLIENT_SECRET, CLIENT_SECRET)
      .put(SalesforceConstants.PROPERTY_USERNAME, USERNAME)
      .put(SalesforceConstants.PROPERTY_PASSWORD, PASSWORD)
      .put(SalesforceConstants.PROPERTY_LOGIN_URL, LOGIN_URL)
      .put(SalesforceConstants.PROPERTY_ERROR_HANDLING, ErrorHandling.STOP.getValue());
  }

  private List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {
    ETLStage source = new ETLStage("SalesforceReader", new ETLPlugin("SalesforceBulk",
                                                                     BatchSource.PLUGIN_TYPE,
                                                                     sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + name.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("SalesforceBulk_" + name.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED,  5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }
}
