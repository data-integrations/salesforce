/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.sink.batch;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

public class StructuredRecordToCSVRecordTransformerTest {

  @Test
  public void testTransform() {
    // Create a Schema field for all datatypes
    Schema schema = Schema.recordOf(
      Schema.Field.of("date_field", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("timestamp_micros_field", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("time_micros_field", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("timestamp_millis_field", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
      Schema.Field.of("time_millis_field", Schema.of(Schema.LogicalType.TIME_MILLIS)),
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)));

    /**
     * Create the record as if coming from the reader
     * Date: 2023-06-12, Time: 07:00:00 GMT
     */
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    recordBuilder.set("date_field", 19520 /*2023-06-12*/);
    recordBuilder.set("timestamp_micros_field", 1686553200000000L /*2023-06-12T07:00:00Z*/);
    recordBuilder.set("time_micros_field", 1686553200000000L /*07:00:00.000Z*/);
    recordBuilder.set("timestamp_millis_field", 1686553200000L /*2023-06-12T07:00:00Z*/);
    recordBuilder.set("time_millis_field", 1686553200000L /*07:00*/);
    recordBuilder.set("string_field", "testValue");
    StructuredRecord record = recordBuilder.build();

    // Converting schema fields to string which can be read by salesforce
    String date = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      record.get("date_field"), schema.getField("date_field"));
    String timestampMicros = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      record.get("timestamp_micros_field"), schema.getField("timestamp_micros_field"));
    String timeMicros = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      record.get("time_micros_field"), schema.getField("time_micros_field"));
    String timestampMillis = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      record.get("timestamp_millis_field"), schema.getField("timestamp_millis_field"));
    String timeMillis = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      record.get("time_millis_field"), schema.getField("time_millis_field"));
    String string = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      record.get("string_field"), schema.getField("string_field"));

    Assert.assertEquals(date, "2023-06-12");
    Assert.assertEquals(timestampMicros, "2023-06-12T07:00:00Z");
    Assert.assertEquals(timeMicros, "07:00:00.000Z");
    Assert.assertEquals(timestampMillis, "2023-06-12T07:00:00Z");
    Assert.assertEquals(timeMillis, "07:00");
    Assert.assertEquals(string, "testValue");

    /**
     * Create the record as if coming from the reader
     * Testing Start Date
     * Date: 0001-01-01, Time: 01:00:00 GMT
     */
    recordBuilder.set("date_field", -719162 /*0001-01-01*/);
    recordBuilder.set("timestamp_micros_field", -62135593200000000L /*0001-01-01T01:00:00Z*/);
    recordBuilder.set("time_micros_field", -62135593200000000L /*01:00:00.000Z*/);
    recordBuilder.set("timestamp_millis_field", -62135593200000L /*0001-01-01T01:00:00Z*/);
    recordBuilder.set("time_millis_field", -62135593200000L /*01:00*/);
    StructuredRecord startRecord = recordBuilder.build();

    // Converting schema fields to string which can be read by salesforce
    String startDate = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      startRecord.get("date_field"), schema.getField("date_field"));
    String startTimestampMicros = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      startRecord.get("timestamp_micros_field"), schema.getField("timestamp_micros_field"));
    String startTimeMicros = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      startRecord.get("time_micros_field"), schema.getField("time_micros_field"));
    String startTimestampMillis = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      startRecord.get("timestamp_millis_field"), schema.getField("timestamp_millis_field"));
    String startTimeMillis = StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(
      startRecord.get("time_millis_field"), schema.getField("time_millis_field"));

    Assert.assertEquals(startDate, "0001-01-01");
    Assert.assertEquals(startTimestampMicros, "0001-01-01T01:00:00Z");
    Assert.assertEquals(startTimeMicros, "01:00:00.000Z");
    Assert.assertEquals(startTimestampMillis, "0001-01-01T01:00:00Z");
    Assert.assertEquals(startTimeMillis, "01:00");
  }
}
