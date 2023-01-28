/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package io.cdap.plugin.salesforce.plugin.source.streaming;

import com.google.common.base.Strings;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.Path;

/**
 * Returns records in realtime created by Salesforce. To achieve this Salesforce Streaming API is used.
 * We use cometd server to subscribe to Salesforce Push Topics and receive realtime updates from there
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SalesforceStreamingSource.NAME)
@Description(SalesforceStreamingSource.DESCRIPTION)
public class SalesforceStreamingSource extends StreamingSource<StructuredRecord> {
  static final String NAME = "Salesforce";
  static final String DESCRIPTION = "Streams data updates from Salesforce using Salesforce Streaming API";
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSource.class);

  private SalesforceStreamingSourceConfig config;

  public SalesforceStreamingSource(SalesforceStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    // Verify that reference name meets dataset id constraints
    IdUtils.validateReferenceName(config.referenceName, collector);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);

    try {
      OAuthInfo oAuthInfo =
        SalesforceConnectionUtil.getOAuthInfo(config, collector);
      if (config.canAttemptToEstablishConnection()) {
        config.validate(collector, oAuthInfo); // validate when macros are not substituted
        config.ensurePushTopicExistAndWithCorrectFields(oAuthInfo); // run when macros are not substituted

        String query = config.getQuery();

        if (!Strings.isNullOrEmpty(query)
          && !config.containsMacro(SalesforceStreamingSourceConfig.PROPERTY_PUSH_TOPIC_QUERY)
          && !config.containsMacro(SalesforceStreamingSourceConfig.PROPERTY_SOBJECT_NAME)
          && config.canAttemptToEstablishConnection()) {

          Schema schema = SalesforceSchemaUtil.getSchema(new AuthenticatorCredentials(oAuthInfo,
                                                                                      config.getConnectTimeout()),
                                                         SObjectDescriptor.fromQuery(query));
          pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
        }
      }
    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      collector.addFailure(String.format("There was issue communicating with Salesforce: %s", message), null)
        .withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(context, config.referenceName, schema,
                    "Read", String.format("Read from Salesforce Stream with push topic of %s.",
                                          config.getPushTopicName()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws ConnectionException {
    FailureCollector collector = streamingContext.getFailureCollector();
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(config, collector);
    config.validate(collector, oAuthInfo); // validate when macros are substituted
    collector.getOrThrowException();

    return SalesforceStreamingSourceUtil.getStructuredRecordJavaDStream(streamingContext, config, oAuthInfo);
  }

  @Path("outputSchema")
  public Schema outputSchema(SalesforceStreamingSourceConfig config) throws Exception {
    AuthenticatorCredentials authenticatorCredentials = config.getAuthenticatorCredentials();
    PartnerConnection partnerConnection = new PartnerConnection(
      Authenticator.createConnectorConfig(authenticatorCredentials));
    SObject pushTopic =
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, config.getPushTopicName());

    String query;
    if (pushTopic == null) {
      // PushTopic is not yet created. This will be done during pipeline run
      query = config.getQuery();
    } else {
      // In case user does not set any query in the corresponding field we need to get it from pushTopic on server
      query = (String) pushTopic.getField("Query");
    }

    return SalesforceSchemaUtil.getSchema(authenticatorCredentials,
                                          SObjectDescriptor.fromQuery(query));
  }

  private void recordLineage(StreamingSourceContext context, String outputName, Schema tableSchema,
                             String operationName, String description)
    throws DatasetManagementException, TransactionFailureException {
    if (tableSchema == null) {
      LOG.warn("Schema for output %s is null. Field-level lineage will not be recorded", outputName);
      return;
    }
    if (tableSchema.getFields() == null) {
      LOG.warn("Schema fields for output %s is empty. Field-level lineage will not be recorded", outputName);
      return;
    }

    context.registerLineage(outputName, tableSchema);
    List<String> fieldNames = tableSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    if (!fieldNames.isEmpty()) {
      LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
      lineageRecorder.recordRead(operationName, description, fieldNames);
    }
  }
}
