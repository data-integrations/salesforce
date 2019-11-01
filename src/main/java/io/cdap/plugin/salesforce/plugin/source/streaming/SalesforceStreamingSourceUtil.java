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

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Salesforce streaming source uti.
 */
final class SalesforceStreamingSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSourceUtil.class);

  static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext streamingContext,
                                                                      SalesforceStreamingSourceConfig config)
    throws ConnectionException {
    config.ensurePushTopicExistAndWithCorrectFields(); // run when macros are substituted

    Schema schema = streamingContext.getOutputSchema();

    if (schema == null) { // if was not set in configurePipeline due to fields containing macro
      schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(),
                                              SObjectDescriptor.fromQuery(config.getQuery()));
    }
    LOG.debug("Schema is {}", schema);

    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

    final Schema finalSchema = schema;
    return jssc.receiverStream(new SalesforceReceiver(config.getAuthenticatorCredentials(),
                                                      config.getPushTopicName()))
      .map(jsonMessage -> getStructuredRecord(jsonMessage, finalSchema))
      .filter(Objects::nonNull);
  }

  private static StructuredRecord getStructuredRecord(String jsonMessage, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    JSONObject sObjectFields;
    try {
      sObjectFields = new JSONObject(jsonMessage) // throws a JSONException if failed to decode
        .getJSONObject("sobject"); // throws a JSONException if not found
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

      builder.set(field.getName(), convertValue(value, field));
    }
    return builder.build();
  }

  private static Object convertValue(Object value, Schema.Field field) {
    if (value == null) {
      return null;
    }

    Schema fieldSchema = field.getSchema();

    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    Schema.Type fieldSchemaType = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    if (fieldSchema.getLogicalType() != null) {
      String valueString = (String) value;
      switch (logicalType) {
        case DATE:
          return Math.toIntExact(ChronoUnit.DAYS.between(Instant.EPOCH, Instant.parse(valueString)));
        case TIMESTAMP_MICROS:
          return TimeUnit.MILLISECONDS.toMicros(Instant.parse(valueString).toEpochMilli());
        case TIME_MICROS:
          return TimeUnit.NANOSECONDS.toMicros(LocalTime.parse(valueString).toNanoOfDay());
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'",
                                                            field.getName(), logicalType.getToken()));
      }
    }

    // Found a single field (Opportunity.Fiscal) which is documented as string and has a string type
    // in describe result, however in Salesforce Streaming API reponse json is represented as json.
    // Converting it and similar back to string, since it does not comply with generated schema.
    if (value instanceof Map) {
      if (fieldSchemaType.equals(Schema.Type.STRING)) {
        return value.toString();
      } else {
        throw new UnexpectedFormatException(
          String.format("Field '%s' is of type '%s', but value found is '%s'",
                        field.getName(), fieldSchemaType.toString(), value.toString()));
      }
    }

    return value;
  }

  private SalesforceStreamingSourceUtil() {
    // no-op
  }
}
