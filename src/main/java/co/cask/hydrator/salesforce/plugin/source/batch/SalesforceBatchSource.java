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

package co.cask.hydrator.salesforce.plugin.source.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.salesforce.SalesforceSchemaUtil;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import co.cask.hydrator.salesforce.parser.SalesforceQueryParser;
import co.cask.hydrator.salesforce.plugin.BaseSalesforceConfig;
import com.google.common.annotations.VisibleForTesting;
import com.sforce.ws.ConnectionException;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
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

  private final Config config;
  private Schema schema;
  private Schema errorSchema;

  SalesforceBatchSource(Config config) throws ConnectionException {
    this.config = config;
  }

  static final class Config extends BaseSalesforceConfig {
    private static final String PROPERTY_QUERY = "query";

    @Description("The SOQL query to retrieve results from")
    @Macro
    private final String query;

    Config(String referenceName, String clientId, String clientSecret,
           String username, String password, String loginUrl, String errorHandling, String query) {
      super(referenceName, clientId, clientSecret, username, password, loginUrl, errorHandling);
      this.query = query;
    }

    Config(Configuration conf) {
        super(null,
              conf.get(SalesforceConstants.CLIENT_ID), conf.get(SalesforceConstants.CLIENT_SECRET),
              conf.get(SalesforceConstants.USERNAME), conf.get(SalesforceConstants.PASSWORD),
              conf.get(SalesforceConstants.LOGIN_URL), null);

        this.query = conf.get(SalesforceConstants.QUERY);
    }

    public String getQuery() {
      return query;
    }

    @Override
    public void validate() {
      super.validate();

      if (!containsMacro(PROPERTY_QUERY)) {
        SalesforceQueryParser.validateQuery(query);
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws ConnectionException {
    this.schema = SalesforceSchemaUtil.getSchemaFromQuery(config.getAuthenticatorCredentials(), config.getQuery());
    this.errorSchema = Schema.recordOf("error",
                                       Schema.Field.of(ERROR_SCHEMA_BODY_PROPERTY, Schema.of(Schema.Type.STRING)));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(); // validate when macros not yet substituted
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws ConnectionException {
    config.validate(); // validate when macros are already substituted
    this.schema = SalesforceSchemaUtil.getSchemaFromQuery(config.getAuthenticatorCredentials(), config.getQuery());

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);

    context.setInput(Input.of(config.referenceName, new SalesforceInputFormatProvider(config)));

    if (schema != null) {
      if (schema.getFields() != null) {
        lineageRecorder.recordRead("Read", "Read from Salesforce",
                                   schema.getFields().stream().map(Schema.Field::getName)
                                     .collect(Collectors.toList()));
      }
    }
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
      switch(config.getErrorHandling()) {
        case Config.ERROR_HANDLING_SKIP:
          break;
        case Config.ERROR_HANDLING_SEND:
          StructuredRecord.Builder builder = StructuredRecord.builder(errorSchema);
          builder.set(ERROR_SCHEMA_BODY_PROPERTY, input.getValue());
          emitter.emitError(new InvalidEntry<StructuredRecord>(400, ex.getMessage(), builder.build()));
          break;
        case Config.ERROR_HANDLING_STOP:
          throw ex;
        default:
          throw new UnexpectedFormatException(
            String.format("Unknown error handling strategy '%s'", config.getErrorHandling()));
      }
    }
  }

  /**
   * Request object for retrieving schema from the Salesforce
   */
  class Request {
    String query;
    String username;
    String password;
    String clientId;
    String clientSecret;
    String loginUrl;
  }

  /**
   * Get Salesforce schema by query.
   *
   * @param request request with credentials and query
   * @return schema calculated from query
   * @throws Exception is thrown by httpclientlib
   */
  @Path("getSchema")
  public Schema getSchema(Request request) throws Exception {
    return SalesforceSchemaUtil.getSchemaFromQuery(new AuthenticatorCredentials(request.username, request.password,
                                                                                request.clientId, request.clientSecret,
                                                                                request.loginUrl), request.query);
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
