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

package io.cdap.salesforce.plugin.source.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.salesforce.SObjectDescriptor;
import io.cdap.salesforce.SalesforceSchemaUtil;
import io.cdap.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.Path;

/**
 * Plugin returns records from Salesforce using provided by user SOQL query.
 * Salesforce bulk API is used to run SOQL query. Bulk API returns data in batches.
 * Every batch is processed as a separate split by mapreduce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchSource.NAME)
@Description("Read data from Salesforce using bulk API.")
public class SalesforceBatchSource extends BatchSource<NullWritable, CSVRecord, StructuredRecord> {
  static final String NAME = "SalesforceBulk";
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBatchSource.class);

  private static final String ERROR_SCHEMA_BODY_PROPERTY = "body";

  private static final Schema errorSchema = Schema.recordOf("error",
    Schema.Field.of(ERROR_SCHEMA_BODY_PROPERTY, Schema.of(Schema.Type.STRING)));

  private final SalesforceSourceConfig config;
  private Schema schema;

  public SalesforceBatchSource(SalesforceSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(); // validate when macros not yet substituted

    if (config.containsMacro(SalesforceSourceConstants.PROPERTY_QUERY)) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
      return;
    }

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(config.getQuery());
    try {
      this.schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(), sObjectDescriptor);
    } catch (ConnectionException e) {
      throw new RuntimeException(String.format("Unable to get schema from query '%s'", config.getQuery()), e);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws ConnectionException {
    config.validate(); // validate when macros are already substituted

    if (schema == null) {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(config.getQuery());
      this.schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(), sObjectDescriptor);
    }

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", "Read from Salesforce",
      Preconditions.checkNotNull(schema.getFields()).stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList()));

    context.setInput(Input.of(config.referenceName, new SalesforceInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    this.schema = context.getOutputSchema();
    super.initialize(context);
  }

  @Override
  public void transform(KeyValue<NullWritable, CSVRecord> input,
                        Emitter<StructuredRecord> emitter) {
    try {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);

      CSVRecord csvRecord = input.getValue();

      if (!csvRecord.isConsistent()) {
        throw new IllegalArgumentException(String.format("CSV record '%s' is not consistent to a csv mapping",
                                                         csvRecord));
      }

      for (Map.Entry<String, String> entry : csvRecord.toMap().entrySet()) {
        String fieldName = entry.getKey();
        String value = entry.getValue();

        Schema.Field field = schema.getField(fieldName);

        if (field == null) {
          continue; // this field is not in schema
        }

        builder.set(fieldName, convertValue(value, field));
      }

      emitter.emit(builder.build());
    } catch (Exception ex) {
      switch (config.getErrorHandling()) {
        case SalesforceSourceConfig.ERROR_HANDLING_SKIP:
          break;
        case SalesforceSourceConfig.ERROR_HANDLING_SEND:
          StructuredRecord.Builder builder = StructuredRecord.builder(errorSchema);
          builder.set(ERROR_SCHEMA_BODY_PROPERTY, input.getValue());
          emitter.emitError(new InvalidEntry<>(400, ex.getMessage(), builder.build()));
          break;
        case SalesforceSourceConfig.ERROR_HANDLING_STOP:
          throw ex;
        default:
          throw new UnexpectedFormatException(
            String.format("Unknown error handling strategy '%s'", config.getErrorHandling()));
      }
    }
  }

  /**
   * Get Salesforce schema by query.
   *
   * @param config Salesforce Source Batch config
   * @return schema calculated from query
   * @throws ConnectionException in case error when establishing connection
   */
  @Path("getSchema")
  public Schema getSchema(SalesforceSourceConfig config) throws ConnectionException {
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(config.getQuery());
    return SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(), sObjectDescriptor);
  }

  private Object convertValue(String value, Schema.Field field) {
    Schema fieldSchema = field.getSchema();

    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    Schema.Type fieldSchemaType = fieldSchema.getType();

    // empty string is considered null in csv, for all types but string.
    if (value.isEmpty() && !fieldSchemaType.equals(Schema.Type.STRING)) {
      return null;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      switch (logicalType) {
        case DATE:
          // date will be in yyyy-mm-dd format
          return Math.toIntExact(LocalDate.parse(value).toEpochDay());
        case TIMESTAMP_MILLIS:
          return Instant.parse(value).toEpochMilli();
        case TIMESTAMP_MICROS:
          return TimeUnit.MILLISECONDS.toMicros(Instant.parse(value).toEpochMilli());
        case TIME_MILLIS:
          return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(LocalTime.parse(value).toNanoOfDay()));
        case TIME_MICROS:
          return TimeUnit.NANOSECONDS.toMicros(LocalTime.parse(value).toNanoOfDay());
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'",
                                                            field.getName(), logicalType.getToken()));
      }
    }

    switch (fieldSchemaType) {
      case NULL:
        return null;
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case INT:
        return Integer.parseInt(value);
      case LONG:
        return Long.parseLong(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case BYTES:
        return Byte.parseByte(value);
      case STRING:
        return value;
    }

    throw new UnexpectedFormatException(
      String.format("Unsupported schema type: '%s' for field: '%s'. Supported types are 'boolean, int, long, float," +
                      "double, binary and string'.", field.getSchema(), field.getName()));
  }

  // testing purposes only
  @VisibleForTesting
  void setSchema(Schema schema) {
    this.schema = schema;
  }
}
