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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Streaming methods for {@link SalesforceStreamingSource}.
 *
 * This class contains methods for {@link SalesforceStreamingSource} that require spark classes because during
 * validation spark classes are not available.
 */

public class SalesforceStreamer {
  private Schema schema;
  private SalesforceStreamingSourceConfig config;

  public SalesforceStreamer(Schema schema, SalesforceStreamingSourceConfig config) {
    this.schema = schema;
    this.config = config;
  }

  public JavaDStream<StructuredRecord> getReceiverStream(StreamingContext streamingContext) {
    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

    return jssc.receiverStream(new SalesforceReceiver(config.getAuthenticatorCredentials(),
                                                      config.getPushTopicName())).
      map((Function<String, StructuredRecord>) this::getStructuredRecord).filter(Objects::nonNull);
  }

  private StructuredRecord getStructuredRecord(String jsonMessage) {
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

  private Object convertValue(Object value, Schema.Field field) {
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
}
