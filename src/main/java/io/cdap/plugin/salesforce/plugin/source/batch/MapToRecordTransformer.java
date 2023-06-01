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

import com.google.cloud.connector.api.data.RecordBuilder;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.SalesforceTransformUtil;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transforms Map of records where key is schema and value is field value
 * into {@link StructuredRecord}.
 */
public class MapToRecordTransformer {

  public StructuredRecord transform(Schema schema, Map<String, ?> record) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    transformRecord(schema, record, builder);
    return builder.build();
  }

  public void transform(Schema schema, Map<String, ?> record, RecordBuilder recordBuilder) {
    // StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    transformRecord(schema, record, recordBuilder);
  }

  private void transformRecord(Schema schema, Map<String, ?> record, StructuredRecord.Builder builder) {
    for (Map.Entry<String, ?> entry : record.entrySet()) {
      String fieldName = SalesforceSchemaUtil.normalizeAvroName(entry.getKey());
      Schema.Field field = schema.getField(fieldName, true);
      if (field == null) {
        continue;
      }
      builder.set(field.getName(), convertValue(field.getName(), entry.getValue(), field.getSchema()));
    }
  }

  private void transformRecord(Schema schema, Map<String, ?> record, RecordBuilder builder) {
    for (Map.Entry<String, ?> entry : record.entrySet()) {
      String fieldName = SalesforceSchemaUtil.normalizeAvroName(entry.getKey());
      Schema.Field field = schema.getField(fieldName, true);
      if (field == null) {
        continue;
      }
      Object val = convertValue(field.getName(), entry.getValue(), field.getSchema());
      if (val == null) {
        builder.field(field.getName()).setNull();
      } else if (val instanceof Boolean) {
        builder.field(field.getName()).set((boolean) val);
      } else if (val instanceof Integer) {
        builder.field(field.getName()).set(((Integer) val).intValue());
      } else if (val instanceof Long) {
        builder.field(field.getName()).set(((Long) val).longValue());
      } else if (val instanceof Float) {
        builder.field(field.getName()).set(((Float) val).floatValue());
      } else if (val instanceof Double) {
        builder.field(field.getName()).set(((Double) val).doubleValue());
      } else if (val instanceof String) {
        builder.field(field.getName()).set((String) val);
      } else {
        throw new UnexpectedFormatException(
            "Connector service unsupported value type: " + val.getClass().getName());
      }
    }
    builder.endStruct();
  }

  private Object convertValue(String fieldName, Object value, Schema fieldSchema) {
    if (fieldSchema.isNullable()) {
      return value == null ? null : convertValue(fieldName, value, fieldSchema.getNonNullable());
    }

    if (value == null) {
      throw new RuntimeException(
          String.format("Found null value for non nullable field %s", fieldName));
    }

    Schema.Type fieldSchemaType = fieldSchema.getType();

    // empty string is considered null in csv
    if (value instanceof String && Strings.isNullOrEmpty((String) value)) {
      return null;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      return SalesforceTransformUtil.transformLogicalType(fieldName, logicalType, String.valueOf(value));
    }

    switch (fieldSchemaType) {
      case NULL:
        return null;
      case BOOLEAN:
        return Boolean.parseBoolean(castValue(value, fieldName, String.class));
      case INT:
        return Integer.parseInt(castValue(value, fieldName, String.class));
      case LONG:
        return Long.parseLong(castValue(value, fieldName, String.class));
      case FLOAT:
        return Float.parseFloat(castValue(value, fieldName, String.class));
      case DOUBLE:
        return Double.parseDouble(castValue(value, fieldName, String.class));
      case STRING:
        return value;
      case RECORD:
        Map<String, String> recordValues = castGeneric(castValue(value, fieldName, Map.class));
        StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(fieldSchema);
        Objects.requireNonNull(fieldSchema.getFields()).forEach(
          field -> transformRecord(fieldSchema, recordValues, nestedBuilder));
        return nestedBuilder.build();
      case ARRAY:
        List<Map<String, String>> list = castGeneric(castValue(value, fieldName, List.class));
        Schema componentSchema = Objects.requireNonNull(fieldSchema.getComponentSchema());
        return list.stream()
          .map(map -> convertValue(fieldName, map, componentSchema))
          .collect(Collectors.toList());
    }

    throw new UnexpectedFormatException(
      String.format("Unsupported schema type: '%s' for field: '%s'. Supported types are 'boolean, int, long, float,"
                      + "double, string, record, array'.", fieldSchema, fieldName));
  }

  private <T> T castValue(Object value, String fieldName, Class<T> clazz) {
    if (clazz.isAssignableFrom(value.getClass())) {
      return clazz.cast(value);
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' is not of expected type '%s'", fieldName, clazz.getSimpleName()));
  }

  @SuppressWarnings("unchecked")
  private <T> T castGeneric(Object value) {
    return (T) value;
  }
}
