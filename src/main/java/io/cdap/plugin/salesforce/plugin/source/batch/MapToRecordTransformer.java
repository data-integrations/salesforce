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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SalesforceTransformUtil;

import java.util.Map;

/**
 * Transforms Map of records where key is field name and value is field value
 * into {@link StructuredRecord}.
 */
public class MapToRecordTransformer {

  public StructuredRecord transform(Schema schema, Map<String, String> record) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Map.Entry<String, String> entry : record.entrySet()) {
      String fieldName = entry.getKey();
      String value = entry.getValue();

      Schema.Field field = schema.getField(fieldName, true);

      if (field == null) {
        continue; // this field is not in schema
      }

      builder.set(field.getName(), convertValue(value, field));
    }

    return builder.build();
  }

  private Object convertValue(String value, Schema.Field field) {
    Schema fieldSchema = field.getSchema();

    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    Schema.Type fieldSchemaType = fieldSchema.getType();

    // empty string is considered null in csv
    if (Strings.isNullOrEmpty(value)) {
      return null;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      return SalesforceTransformUtil.transformLogicalType(field.getName(), logicalType, value);
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
      case STRING:
        return value;
    }

    throw new UnexpectedFormatException(
      String.format("Unsupported schema type: '%s' for field: '%s'. Supported types are 'boolean, int, long, float," +
        "double and string'.", field.getSchema(), field.getName()));
  }

}
