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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.source.SalesforceDStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Salesforce streaming source uti.
 */
public final class SalesforceStreamingSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSourceUtil.class);
  private static final Gson gson = new Gson();

  static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext streamingContext,
                                                                      SalesforceStreamingSourceConfig config)
          throws Exception {
    config.ensurePushTopicExistAndWithCorrectFields(); // run when macros are substituted

    Schema schema = streamingContext.getOutputSchema();

    if (schema == null) { // if was not set in configurePipeline due to fields containing macro
      if (config.getConnection() != null) {
        schema = SalesforceSchemaUtil.getSchema(config.getConnection().getAuthenticatorCredentials(),
                                                SObjectDescriptor.fromQuery(config.getQuery()));
      }
    }
    LOG.debug("Schema is {}", schema);

    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

    final Schema finalSchema = schema;

    /************************************* Receiver based ***************************************/

    /*InputDStream inputDStream = jssc.receiverStream(new SalesforceReceiver(config.getConnection()
      .getAuthenticatorCredentials(), config.getPushTopicName(), getState(streamingContext, config))).inputDStream();*/

    /************************************* Direct Streaming based ***************************************/

    InputDStream inputDStream = new DirectSalesforceInputDStream(jssc.ssc(),
            config, config.getConnection().getAuthenticatorCredentials(), getState(streamingContext, config));

    /*ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
    return new JavaDStream<>(inputDStream, tag);*/

    SalesforceDStream salesforceDStream = new SalesforceDStream(jssc.ssc(), inputDStream,
            new BytesFunction(config, schema), getStateConsumer(streamingContext, config));
    return salesforceDStream.convertToJavaDStream();
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

  private static ConcurrentMap<String, Integer> getState(StreamingContext streamingContext,
    SalesforceStreamingSourceConfig config) throws IOException {
    Integer replayId = -1;
    ConcurrentMap<String, Integer> replay = new ConcurrentHashMap<>();

    //State store is not enabled, do not read state

    if (!streamingContext.isStateStoreEnabled()) {
      replay.put("/topic/" + config.getPushTopicName(), replayId);
      return replay;
    }

    //If state is not present, use configured repayId or defaults
    Optional<byte[]> state = streamingContext.getState(config.getPushTopicName());
    if (!state.isPresent()) {
      LOG.info("No saved state found.");
      replay.put("/topic/" + config.getPushTopicName(), replayId);
      return replay;
    }

    byte[] bytes = state.get();
    try (Reader reader = new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
      replayId = gson.fromJson(reader, Integer.class);
      LOG.info("Saved state found. ReplayId = {}. ", replayId);
      if (replay.putIfAbsent("/topic/" + config.getPushTopicName(), replayId) != null) {
        throw new IllegalStateException(String.format("Already subscribed to %s",
                                                      config.getPushTopicName()));
      }
      return replay;
    }
  }

  private static VoidFunction<Object> getStateConsumer(StreamingContext context,
                                                        SalesforceStreamingSourceConfig config) {
    return replayId -> saveState(context, (int) replayId, config);
  }
  private static void saveState(StreamingContext streamingContext, int replayId,
                                SalesforceStreamingSourceConfig config) throws IOException {
      if (replayId > 0) {
        byte[] state = gson.toJson(replayId).getBytes(StandardCharsets.UTF_8);
        streamingContext.saveState(config.getPushTopicName(), state);
        LOG.info("State saved. ReplayId = {}. ", replayId);
      }
  }
  
  static class RecordTransform
          implements Function2<JavaRDD<String>, Time, JavaRDD<StructuredRecord>> {

    private final SalesforceStreamingSourceConfig conf;
    private final Schema outputSchema;

    RecordTransform(SalesforceStreamingSourceConfig conf, Schema outputSchema) {
      this.conf = conf;
      this.outputSchema = outputSchema;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<String> input, Time batchTime) {
      Function2<String, Time, StructuredRecord> recordFunction =
              new BytesFunction(conf, outputSchema);
      return input.map((Function<String, StructuredRecord>) jsonRecord ->
              recordFunction.call(jsonRecord, batchTime));
    }
  }

  /**
   * Common logic for transforming record into a structured record.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  public static class BytesFunction implements Function2<String, Time, StructuredRecord> {
    protected final SalesforceStreamingSourceConfig conf;
    private final Schema outputSchema;

    BytesFunction(SalesforceStreamingSourceConfig conf, Schema outputSchema) {
      this.conf = conf;
      this.outputSchema = outputSchema;
    }

    @Override
    public StructuredRecord call(String in, Time batchTime) {
      return getStructuredRecord(in, this.outputSchema);
    }
  }

}
