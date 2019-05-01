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
import com.sforce.soap.partner.sobject.SObject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ProgramManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSource;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@inheritDoc}
 */
public abstract class BaseSalesforceStreamingSourceETLTest extends BaseSalesforceETLTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSalesforceStreamingSourceETLTest.class);
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-streams", "1.0.0");
  private static final int SALESFORCE_RECEIVER_START_TIMEOUT_SECONDS = 60;
  private static final long SALESFORCE_RECEIVER_START_POLLING_INTERVAL_MS = 100;
  private static final String SALESFORCE_RECEIVER_NAME_PREFIX = "salesforce_streaming_api_listener";
  private static final int WAIT_FOR_RECORDS_TIMEOUT_SECONDS = 60;
  private static final long WAIT_FOR_RECORDS_POLLING_INTERVAL_MS = 100;

  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                          Constants.AppFabric.SPARK_COMPAT, Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    LOG.info("Setting up application");

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);

    LOG.info("Setting up plugins");

    addPluginArtifact(NamespaceId.DEFAULT.artifact("salesforce-plugins", "1.0.0"),
                      APP_ARTIFACT_ID,
                      SalesforceStreamingSource.class,
                      AuthenticatorCredentials.class
    );
  }

  protected ProgramManager startPipeline(Map<String, String> properties) throws Exception {
    String pushTopicName = testName.getMethodName();

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .putAll(getBaseProperties("SalesforceStreaming-input").build())
      .put("pushTopicName", pushTopicName)
      .put("pushTopicNotifyCreate", "Enabled")
      .put("pushTopicNotifyUpdate", "Enabled")
      .put("pushTopicNotifyDelete", "Enabled")
      .put("pushTopicNotifyForFields", "Referenced")
      .putAll(properties)
      .build();

    ETLPlugin sourceConfig = new ETLPlugin("Salesforce", StreamingSource.PLUGIN_TYPE, sourceProps);
    ETLPlugin sinkConfig = MockSink.getPlugin(getOutputDatasetName());

    ProgramManager programManager =
      deployETL(sourceConfig, sinkConfig, "SalesforceStreaming_" + testName.getMethodName());
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 30, TimeUnit.SECONDS);

    // Wait for Spark to start up the job by checking if receiver thread exists
    Awaitility.await()
      .atMost(SALESFORCE_RECEIVER_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      .pollInterval(SALESFORCE_RECEIVER_START_POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS)
      .untilAsserted(() -> Assert.assertTrue(
        "Salesforce receiver thread has not started", this.hasReceiverThreadStarted()));

    SObject pushTopic = SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, pushTopicName);
    createdObjectsIds.add((String) pushTopic.getField("Id"));

    return programManager;
  }

  protected String getOutputDatasetName() {
    return "output-realtimesourcetest_" + testName.getMethodName();
  }

  protected List<StructuredRecord> waitForRecords(ProgramManager programManager,
                                                  int exceptedNumberOfRecords) throws Exception {
    DataSetManager<Table> outputManager = getDataset(getOutputDatasetName());

    Awaitility.await()
      .atMost(WAIT_FOR_RECORDS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      .pollInterval(WAIT_FOR_RECORDS_POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS)
      .untilAsserted((() -> {
        int recordsCount = MockSink.readOutput(outputManager).size();
        Assert.assertTrue(
          String.format("At least %d records expected, but %d found", exceptedNumberOfRecords, recordsCount),
          recordsCount >= exceptedNumberOfRecords);
      }));

    programManager.stop();
    programManager.waitForStopped(10, TimeUnit.SECONDS);
    programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);

    return MockSink.readOutput(outputManager);
  }

  private SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName) throws Exception {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    return applicationManager.getSparkManager(DataStreamsSparkLauncher.NAME);
  }

  /**
   * Checks if Spark Receiver has started by checking if spark_salesforce_receiver thread started.
   * This usually happens in ~15 seconds after pipeline status changes to "running".
   *
   * We need this to understand when Spark starts listening for events. So we can submit data to
   * Salesforce and receive data.
   *
   * @return true if receiver thread started
   */
  private boolean hasReceiverThreadStarted() {
    Set<String> resultSet = Thread.getAllStackTraces().keySet()
      .stream()
      .map(Thread::getName)
      .filter(name -> name.startsWith(SALESFORCE_RECEIVER_NAME_PREFIX))
      .collect(Collectors.toSet());

    return !resultSet.isEmpty();
  }
}
