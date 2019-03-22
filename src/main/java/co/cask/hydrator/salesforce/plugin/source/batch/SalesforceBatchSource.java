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
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.salesforce.plugin.BaseSalesforceConfig;
import co.cask.hydrator.salesforce.SalesforceSchemaUtil;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import co.cask.hydrator.salesforce.parser.SalesforceQueryParser;
import com.google.common.annotations.VisibleForTesting;
import com.sforce.ws.ConnectionException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.util.stream.Collectors;
import javax.ws.rs.Path;


/**
 * Plugin returns records from Salesforce using provided by user SOQL query.
 * Salesforce bulk API is used to run SOQL query. Bulk API returns data in batches.
 * Every batch is processed as a separate split by mapreduce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchSource.NAME)
@Description("Plugin to read data from Salesforce in batches.")
public class SalesforceBatchSource extends BatchSource<String, String, StructuredRecord> {
  static final String NAME = "SalesforceBatchSource";
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBatchSource.class);

  private final Config config;
  private Schema schema;

  SalesforceBatchSource(Config config) throws ConnectionException {
    this.config = config;
  }

  static final class Config extends BaseSalesforceConfig {
    private static final String PROPERTY_QUERY = "query";

    @Description("The SOQL query to retrieve results from")
    @Macro
    private final String query;

    Config(Configuration conf) {
        super(null,
              conf.get(SalesforceConstants.CLIENT_ID), conf.get(SalesforceConstants.CLIENT_SECRET),
              conf.get(SalesforceConstants.USERNAME), conf.get(SalesforceConstants.PASSWORD),
              conf.get(SalesforceConstants.LOGIN_URL));

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
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(); // validate when macros not yet substituted
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws ConnectionException {
    config.validate(); // validate when macros are already substituted

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
  public void transform(KeyValue<String, String> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    String[] fieldNames = getValuesFromCSVRow(input.getKey());
    String[] values = getValuesFromCSVRow(input.getValue());

    if (fieldNames.length != values.length) {
      throw new IllegalArgumentException("Number of fields is not equal to the number of values");
    }

    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      String value = values[i];

      Schema.Field field = schema.getField(fieldName);

      if (field == null) {
        continue; // this field is not in schema
      }

      builder.set(fieldName, convertValue(value, field));
    }

    emitter.emit(builder.build());
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

  /**
   * An advanced version of split csv row by comma. We cannot simply split by comma since some values may have comma
   * inside them and Salesforce does not escape it.
   *
   * The format of Salesforce csv row is:
   * "value1","value2","value3"
   *
   * So we are splitting by "\",\"" instead of by comma. Since quotes are not allowed without escaping
   * in values this works in any case.
   *
   *
   * @param csvRow one row in csv format with quoted values
   * @return an array of values
   */
  private String[] getValuesFromCSVRow(String csvRow) {
    String[] values = csvRow.split("\",\"");

    values[0] = values[0].substring(1);

    String last = values[values.length - 1];
    values[values.length - 1] = last.substring(0, last.length() - 1);

    return values;
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
