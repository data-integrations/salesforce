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
import co.cask.hydrator.salesforce.plugin.source.batch.SalesforceBatchSource;
import com.google.common.collect.ImmutableMap;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.internal.AssumptionViolatedException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Methods to run ETL with Salesforce Bulk plugin as source, and a mock plugin as a sink.
 *
 * By default all tests will be skipped, since Salesforce credentials are needed.
 *
 * Instructions to enable the tests:
 * 1. Create/use existing Salesforce account
 * 2. Tables need to have some records. Defaults setup by Salesforce during account creation are enough.
 * 2. Create connected application within the account to get clientId and clientSecret
 * 3. Run the tests using the command below:
 *
 * mvn clean test -Dsf.test.clientId= -Dsf.test.clientSecret= -Dsf.test.username= -Dsf.test.password=
 *
 */
public abstract class BaseSalesforceBatchSourceETLTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  // TODO: move to properties
  // TODO: ignore if not set
  private static final String CLIENT_ID = System.getProperty("sf.test.clientId");
  private static final String CLIENT_SECRET = System.getProperty("sf.test.clientSecret");
  private static final String USERNAME = System.getProperty("sf.test.username");
  private static final String PASSWORD = System.getProperty("sf.test.password");
  private static final String LOGIN_URL = System.getProperty("sf.test.loginUrl",
                                                            "https://login.salesforce.com/services/oauth2/token");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD, LOGIN_URL);
    } catch (AssumptionViolatedException e) {
      System.out.println("WARNING: ETL tests are skipped. Please find the instructions on enabling it at" +
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
  }

  protected List<StructuredRecord> getResultsBySOQLQuery(String query) throws Exception {
    ImmutableMap.Builder<String, String> propsBulder = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "SalesforceBulk" + UUID.randomUUID().toString())
      .put("clientId", CLIENT_ID)
      .put("clientSecret", CLIENT_SECRET)
      .put("username", USERNAME)
      .put("password", PASSWORD)
      .put("loginUrl", LOGIN_URL)
      .put("query", query);

    return getPipelineResults(propsBulder.build());
  }

  private List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {
    ETLStage source = new ETLStage("SalesforceReader", new ETLPlugin("SalesforceBulk",
                                                                     BatchSource.PLUGIN_TYPE,
                                                                     sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest" + UUID.randomUUID().toString();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testPipeline" + UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    return outputRecords;
  }
}
