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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class MapToRecordTransformerTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testTransform() {
    Schema schema = Schema.recordOf("output",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("boolean_field", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("date_field", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("timestamp_field", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("array_field", Schema.arrayOf(Schema.recordOf("array_field",
        Schema.Field.of("nested_string", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("nested_long", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("nested_timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))))));

    // Create the record as if coming from the reader. Field names can contains "." for composite table.
    // After the transform, they should all mapped to Avro allowed names with "." replaced with "_".
    ImmutableMap<String, Object> records = ImmutableMap.<String, Object>builder()
      .put("string.field", "testValue")
      .put("int.field", "1")
      .put("long.field", "12345678912345")
      .put("float_field", "1.1")
      .put("double_field", "2.023")
      .put("boolean_field", "true")
      .put("date_field", "2019-05-20")
      .put("timestamp_field", "2019-05-20T01:01:01Z")
      .put("array_field", ImmutableList.builder()
        .add(ImmutableMap.builder()
               .put("nested_string", "testNestedValue")
               .put("nested_long", "123456789123456")
               .put("nested_timestamp", "2019-03-19T22:22:22Z")
               .build()).build()
      ).build();

    MapToRecordTransformer recordTransformer = new MapToRecordTransformer();
    StructuredRecord structuredRecord = recordTransformer.transform(schema, records);
    Assert.assertEquals(records.get("string.field"), structuredRecord.get("string_field"));
    Assert.assertEquals(Integer.valueOf((String) records.get("int.field")), structuredRecord.get("int_field"));
    Assert.assertEquals(Long.valueOf((String) records.get("long.field")), structuredRecord.get("long_field"));
    Assert.assertEquals(Float.valueOf((String) records.get("float_field")), structuredRecord.get("float_field"));
    Assert.assertEquals(Double.valueOf((String) records.get("double_field")), structuredRecord.get("double_field"));
    Assert.assertEquals(Boolean.valueOf((String) records.get("boolean_field")), structuredRecord.get("boolean_field"));
    Assert.assertEquals(LocalDate.parse((String) records.get("date_field"), DateTimeFormatter.ISO_DATE),
                        structuredRecord.getDate("date_field"));
    Assert.assertEquals(ZonedDateTime.parse((String) records.get("timestamp_field"), DateTimeFormatter.ISO_DATE_TIME),
                        structuredRecord.getTimestamp("timestamp_field", ZoneOffset.UTC));

    List<Map<String, String>> arrayField = (List<Map<String, String>>) records.get("array_field");
    Assert.assertEquals(1, arrayField.size());

    List<StructuredRecord> arrayRecord = structuredRecord.get("array_field");
    Assert.assertNotNull(arrayRecord);
    Assert.assertEquals(1, arrayRecord.size());

    Assert.assertEquals(arrayField.get(0).get("nested_string"), arrayRecord.get(0).get("nested_string"));
    Assert.assertEquals(Long.valueOf(arrayField.get(0).get("nested_long")), arrayRecord.get(0).get("nested_long"));
    Assert.assertEquals(ZonedDateTime.parse(arrayField.get(0).get("nested_timestamp"), DateTimeFormatter.ISO_DATE_TIME),
                        arrayRecord.get(0).getTimestamp("nested_timestamp", ZoneOffset.UTC));
  }
}
