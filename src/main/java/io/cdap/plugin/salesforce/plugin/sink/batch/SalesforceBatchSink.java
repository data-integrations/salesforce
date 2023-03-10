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
package io.cdap.plugin.salesforce.plugin.sink.batch;

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.apache.hadoop.io.NullWritable;

import java.util.stream.Collectors;

/**
 * Plugin inserts records into Salesforce using Salesforce Bulk API.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SalesforceBatchSink.PLUGIN_NAME)
@Description("Writes records to Salesforce")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = SalesforceConstants.PLUGIN_NAME)})
public class SalesforceBatchSink extends BatchSink<StructuredRecord, NullWritable, CSVRecord> {

  public static final String PLUGIN_NAME = "Salesforce";
  private final SalesforceSinkConfig config;
  private StructuredRecordToCSVRecordTransformer transformer;

  public SalesforceBatchSink(SalesforceSinkConfig config) throws ConnectionException {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    if (!config.getConnection().canAttemptToEstablishConnection()) {
      config.validateSinkProperties(collector);
      return;
    }
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(config.getConnection().getAuthenticatorCredentials(),
                                                                collector);
    config.validate(stageConfigurer.getInputSchema(), stageConfigurer.getFailureCollector(), oAuthInfo);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Schema inputSchema = context.getInputSchema();
    FailureCollector collector = context.getFailureCollector();
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(config.getConnection().getAuthenticatorCredentials(),
                                                                collector);
    config.validate(inputSchema, collector, oAuthInfo);
    collector.getOrThrowException();
    String orgId = "unknown";
    try {
      orgId = config.getOrgId(oAuthInfo);
    } catch (ConnectionException exception) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(exception);
      collector.addFailure(String.format("Unable to get organization Id due to error: %s", message),
                           "Ensure Credentials are correct.");
    }
    Asset asset = Asset.builder(config.getReferenceNameOrNormalizedFQN(orgId, config.getSObject()))
      .setFqn(config.getFQN(orgId, config.getSObject())).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      String operationDescription = String.format("Wrote to Salesforce %s", config.getSObject());
      lineageRecorder.recordWrite("Write", operationDescription,
                                  inputSchema.getFields().stream()
                                    .map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
    }
    context.addOutput(Output.of(config.getReferenceNameOrNormalizedFQN(orgId, config.getSObject()),
                                new SalesforceOutputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new StructuredRecordToCSVRecordTransformer();
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<NullWritable, CSVRecord>> emitter) {
    CSVRecord csvRecord = transformer.transform(record);
    emitter.emit(new KeyValue<>(null, csvRecord));
  }
}
