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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Plugin returns records from Salesforce using provided by user SOQL query or SObject.
 * Reads data in batches, every batch is processed as a separate split by mapreduce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchSource.NAME)
@Description("Read data from Salesforce.")
public class SalesforceBatchSource extends BatchSource<Schema, Map<String, String>, StructuredRecord> {


  public static final String NAME = "Salesforce";

  private final SalesforceSourceConfig config;
  private Schema schema;
  private MapToRecordTransformer transformer;

  public SalesforceBatchSource(SalesforceSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // validate when macros not yet substituted
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());

    if (config.containsMacro(SalesforceSourceConstants.PROPERTY_SCHEMA)) {
      // schema will be available later during `prepareRun` stage
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
      return;
    }

    if (config.containsMacro(SalesforceSourceConstants.PROPERTY_QUERY)
      || config.containsMacro(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)
      || !config.canAttemptToEstablishConnection()) {
      // some config properties required for schema generation are not available
      // will validate schema later in `prepareRun` stage
      pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
      return;
    }

    schema = retrieveSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector); // validate when macros are already substituted
    collector.getOrThrowException();

    if (schema == null) {
      schema = retrieveSchema();
    }

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", "Read from Salesforce",
      Preconditions.checkNotNull(schema.getFields()).stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList()));

    String query = config.getQuery(context.getLogicalStartTime());
    String sObjectName = SObjectDescriptor.fromQuery(query).getName();
    context.setInput(Input.of(config.referenceName, new SalesforceInputFormatProvider(config,
        Collections.singletonList(query), ImmutableMap.of(sObjectName, schema.toString()), null)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new MapToRecordTransformer();
  }

  @Override
  public void transform(KeyValue<Schema, Map<String, String>> input,
                        Emitter<StructuredRecord> emitter) {
    try {
      StructuredRecord record = transformer.transform(input.getKey(), input.getValue());
      emitter.emit(record);
    } catch (Exception e) {
      // hacky check to see if this is a header placed in the middle of all the data due to chunking
      Set<String> schemaFieldNames = input.getKey().getFields().stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toSet());
      Set<String> columnValues = new HashSet<>(input.getValue().values());
      if (columnValues.containsAll(schemaFieldNames)) {
        // looks like a header, ignore it
        // can potentially fail if the configured schema has extra fields that are not in the data
        // but in those cases, those fields should probably be removed anyway.
        return;
      }
      throw e;
    }
  }

  /**
   * Get Salesforce schema by query.
   *
   * @param config Salesforce Source Batch config
   * @return schema calculated from query
   */
  private Schema getSchema(SalesforceSourceConfig config) {
    String query = config.getQuery(System.currentTimeMillis());
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    try {
      return SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(), sObjectDescriptor);
    } catch (ConnectionException e) {
      throw new RuntimeException(String.format("Unable to get schema from the query '%s'", query), e);
    }
  }

  /**
   * Retrieves provided and actual schemas.
   * If both schemas are available, validates their compatibility.
   *
   * @return provided schema if present, otherwise actual schema
   */
  private Schema retrieveSchema() {
    Schema providedSchema = config.getSchema();
    Schema actualSchema = getSchema(config);
    if (providedSchema != null) {
      SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
      return providedSchema;
    }
    return actualSchema;
  }
}
