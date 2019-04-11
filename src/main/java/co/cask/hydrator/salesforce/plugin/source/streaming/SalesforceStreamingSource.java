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

package co.cask.hydrator.salesforce.plugin.source.streaming;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.api.validation.InvalidStageException;
import co.cask.hydrator.plugin.spark.ReferenceStreamingSource;
import co.cask.hydrator.salesforce.SObjectDescriptor;
import co.cask.hydrator.salesforce.SalesforceSchemaUtil;
import co.cask.hydrator.salesforce.authenticator.Authenticator;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import com.google.common.base.Strings;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import javax.ws.rs.Path;

/**
 * Returns records in realtime created by Salesforce. To achieve this Salesforce Streaming API is used.
 * We use cometd server to subscribe to Salesforce Push Topics and receive realtime updates from there
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SalesforceStreamingSource.NAME)
@Description(SalesforceStreamingSource.DESCRIPTION)
public class SalesforceStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSource.class);

  static final String NAME = "SalesforceStreaming";
  static final String DESCRIPTION = "Salesforce Streaming";
  private SalesforceStreamingSourceConfig config;
  private Schema schema;

  public SalesforceStreamingSource(SalesforceStreamingSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    try {
      config.validate(); // validate when macros are not substituted
      config.ensurePushTopicExistAndWithCorrectFields(); // run when macros are not substituted

      String query = config.getQuery();

      if (!Strings.isNullOrEmpty(query)
        && !config.containsMacro(SalesforceStreamingSourceConfig.PROPERTY_PUSHTOPIC_QUERY)
        && !config.containsMacro(SalesforceStreamingSourceConfig.PROPERTY_SOBJECT_NAME)) {

        Schema schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(),
                                                       SObjectDescriptor.fromQuery(query));
        pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      }
    } catch (ConnectionException e) {
      throw new InvalidStageException("There was issue communicating with Salesforce", e);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws ConnectionException {
    config.validate(); // validate when macros are substituted
    config.ensurePushTopicExistAndWithCorrectFields(); // run when macros are substituted

    this.schema = streamingContext.getOutputSchema();

    if (this.schema == null) { // if was not set in configurePipeline due to fields containing macro
      this.schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(),
                                                   SObjectDescriptor.fromQuery(config.getPushTopicQuery()));
    }
    LOG.debug("Schema is {}", schema);

    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

    return jssc.receiverStream(new SalesforceReceiver(this.config.getAuthenticatorCredentials(),
                                                      this.config.getPushTopicName())).
      map((Function<String, StructuredRecord>) this::getStructuredRecord).filter(Objects::nonNull);
  }

  private StructuredRecord getStructuredRecord(String jsonMessage) throws ConnectionException {
    try {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);

      JSONObject sObjectFields;
      try {
        sObjectFields = new JSONObject(jsonMessage) // throws a JSONException if failed to decode
          .getJSONObject("data") // throws a JSONException if not found
          .getJSONObject("sobject");
      } catch (JSONException e) {
        throw new IllegalStateException(
          String.format("Cannot retrieve /data/sobject from json message %s", jsonMessage), e);
      }

      for (Map.Entry<String, Object> entry : sObjectFields.toMap().entrySet()) {
        String fieldName = entry.getKey();
        Object value = entry.getValue();

        Schema.Field field = schema.getField(fieldName, true);

        if (field == null) {
          continue; // this field is not in schema
        }

        builder.set(field.getName(), value);
      }
      return builder.build();
    } catch (Exception ex) {
      switch (config.getErrorHandling()) {
        case SKIP:
          LOG.warn("Cannot process json '{}', skipping it.", jsonMessage, ex);
          return null;
        case STOP:
          throw ex;
        default:
          throw new UnexpectedFormatException(
            String.format("Unknown error handling strategy '%s'", config.getErrorHandling()));
      }
    }
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
}
