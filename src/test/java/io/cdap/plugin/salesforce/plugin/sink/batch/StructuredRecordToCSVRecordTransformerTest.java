/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.plugin.source.batch.MapToRecordTransformer;
import org.junit.Assert;
import org.junit.Test;

public class StructuredRecordToCSVRecordTransformerTest {
  @Test
  public void testConvertSchemaFieldToStringWithNulValues() {
    Assert.assertNull(StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString(null, null));
  }

  @Test
  public void testConvertSchemaFieldToString() {
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsDeleted", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("ExpectedRevenue", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("LastModifiedDate", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("Time", Schema.of(Schema.LogicalType.TIME_MICROS))
    );
    Schema.Field field;
    field = schema.getField("Id");
    Assert.assertNotNull(StructuredRecordToCSVRecordTransformer.convertSchemaFieldToString("42", field));
  }

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

    ImmutableMap<String, Object> records = ImmutableMap.<String, Object>builder()
      .put("string_field", "testValue")
      .put("int_field", "1")
      .put("long_field", "12345678912345")
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
    StructuredRecordToCSVRecordTransformer structuredRecordToCSVRecordTransformer =
      new StructuredRecordToCSVRecordTransformer();
    CSVRecord csvRecord = structuredRecordToCSVRecordTransformer.transform(structuredRecord);
    Assert.assertNotNull(csvRecord.getValues());
    Assert.assertNotNull(csvRecord.getColumnNames());
  }
}
