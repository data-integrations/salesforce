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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.salesforce.SalesforceConstants;
import co.cask.hydrator.salesforce.authenticator.Authenticator;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import co.cask.hydrator.salesforce.plugin.ErrorHandling;
import co.cask.hydrator.salesforce.plugin.source.batch.SalesforceBatchSource;
import co.cask.hydrator.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import com.google.common.collect.ImmutableMap;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
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

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static final String CLIENT_ID = System.getProperty("salesforce.test.clientId");
  private static final String CLIENT_SECRET = System.getProperty("salesforce.test.clientSecret");
  private static final String USERNAME = System.getProperty("salesforce.test.username");
  private static final String PASSWORD = System.getProperty("salesforce.test.password");
  private static final String LOGIN_URL = System.getProperty("salesforce.test.loginUrl",
                                                             "https://login.salesforce.com/services/oauth2/token");

  private List<SaveResult> createdObjectsIds = new ArrayList<>();
  private static PartnerConnection partnerConnection;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD, LOGIN_URL);
    } catch (AssumptionViolatedException e) {
      LOG.info("WARNING: ETL tests are skipped. Please find the instructions on enabling it at" +
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
                      SalesforceBatchSource.class);

    partnerConnection = new PartnerConnection(Authenticator.createConnectorConfig(
      new AuthenticatorCredentials(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, LOGIN_URL)));
  }

  @After
  public void clearSObjects() throws ConnectionException {
    String[] ids = createdObjectsIds
      .stream()
      .map(SaveResult::getId)
      .collect(Collectors.toList())
      .toArray(new String[createdObjectsIds.size()]);

    partnerConnection.delete(ids);
  }

  /**
   * Adds sObjects to Salesforce.
   * Checks the result response for errors.
   * Saves the objects so that they can be deleted after method is run.
   *
   * @param sObjects list of sobjects to create
   */
  void addSObjects(List<SObject> sObjects) {
    try {
      SaveResult[] results = partnerConnection.create(sObjects.toArray(new SObject[0]));
      createdObjectsIds.addAll(Arrays.asList(results));

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
