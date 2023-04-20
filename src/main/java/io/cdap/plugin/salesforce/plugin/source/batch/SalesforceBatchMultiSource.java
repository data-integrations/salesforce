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
package io.cdap.plugin.salesforce.plugin.source.batch;

import com.sforce.async.BulkConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSplitUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Batch source to read multiple SObjects from Salesforce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchMultiSource.NAME)
@Description("Reads multiple SObjects in Salesforce. "
  + "Outputs one record for each row in each SObject, with the SObject name as a record field. "
  + "Also sets a pipeline argument for each SObject read, which contains its schema.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = SalesforceConstants.PLUGIN_NAME)})
public class SalesforceBatchMultiSource extends BatchSource<Schema, Map<String, String>, StructuredRecord> {

  public static final String NAME = "SalesforceMultiObjects";

  private static final String MULTI_SINK_PREFIX = "multisink.";

  private final SalesforceMultiSourceConfig config;
  private MapToRecordTransformer transformer;
  private Set<String> jobIds = new HashSet<>();
  private AuthenticatorCredentials authenticatorCredentials;

  public SalesforceBatchMultiSource(SalesforceMultiSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(config.getConnection(), collector);
    config.validate(stageConfigurer.getFailureCollector(), oAuthInfo); // validate before macros are substituted
    config.validateSObjects(stageConfigurer.getFailureCollector(), oAuthInfo);
    stageConfigurer.setOutputSchema(null);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws ConnectionException {
    FailureCollector collector = context.getFailureCollector();
    config.validateFilters(collector);
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(config.getConnection(), collector);
    config.validate(collector, oAuthInfo);
    config.validateSObjects(collector, oAuthInfo);
    collector.getOrThrowException();

    List<String> queries = config.getQueries(context.getLogicalStartTime(), oAuthInfo);
    Map<String, Schema> schemas = config.getSObjectsSchemas(queries);

    // propagate schema for each SObject for multi sink plugin
    SettableArguments arguments = context.getArguments();
    schemas.forEach(
      (sObjectName, sObjectSchema) -> arguments.set(MULTI_SINK_PREFIX + sObjectName, sObjectSchema.toString()));
    String sObjectNameField = config.getSObjectNameField();
    authenticatorCredentials = config.getConnection().getAuthenticatorCredentials();
    BulkConnection bulkConnection = SalesforceSplitUtil.getBulkConnection(authenticatorCredentials);
    List<SalesforceSplit> querySplits = queries.parallelStream()
      .map(query -> SalesforceSplitUtil.getQuerySplits(query, bulkConnection, false, config.getOperation()))
      .flatMap(Collection::stream).collect(Collectors.toList());
    // store the jobIds so be used in onRunFinish() to close the connections
    querySplits.parallelStream().forEach(salesforceSplit -> jobIds.add(salesforceSplit.getJobId()));
    context.setInput(Input.of(config.referenceName, new SalesforceInputFormatProvider(
      config, getSchemaWithNameField(sObjectNameField, schemas), querySplits, sObjectNameField)));
    /* TODO PLUGIN-510
     *  As part of [CDAP-16290], recordLineage function was introduced with out implementation.
     *  To avoid compilation errors the code block is commented for future fix.
    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(context, config.referenceName, schema,
                    "Read", "Read from Salesforce MultiObjects.");
    }
     */
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
    SalesforceSplitUtil.closeJobs(jobIds, authenticatorCredentials);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new MapToRecordTransformer();
  }

  @Override
  public void transform(KeyValue<Schema, Map<String, String>> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord record = transformer.transform(input.getKey(), input.getValue());
    emitter.emit(record);
  }

  /**
   * For each given schema adds name field of type String and converts it to string representation.
   *
   * @param sObjectNameField sObject field name
   * @param schemas          map of schemas where key is SObject name to which value schema corresponds
   * @return schema with named field
   */
  private Map<String, String> getSchemaWithNameField(String sObjectNameField, Map<String, Schema> schemas) {
    return schemas.entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> getSchemaString(sObjectNameField, entry.getValue()),
        (o, n) -> n));
  }

  /**
   * Adds sObject name field to the given schema and converts it to string representation.
   *
   * @param sObjectNameField sObject name field
   * @param schema           CDAP schema
   * @return updated schema in string representation
   */
  private String getSchemaString(String sObjectNameField, Schema schema) {
    if (schema.getType() != Schema.Type.RECORD || schema.getFields() == null) {
      throw new IllegalArgumentException(String.format("Invalid schema '%s'", schema));
    }
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    fields.add(Schema.Field.of(sObjectNameField, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf(Objects.requireNonNull(schema.getRecordName()), fields).toString();
  }
}
