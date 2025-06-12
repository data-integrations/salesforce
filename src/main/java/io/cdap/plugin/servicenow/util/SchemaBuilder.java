/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.servicenow.util;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utility class to build schema.
 */
public class SchemaBuilder {
  /**
   * Constructs Schema object using input parameters.
   *
   * @param tableName The table name to be used in Schema object
   * @param columns   The list of ServiceNowColumn objects that will be added as Schema.Field
   * @return The instance of Schema object
   */
  public static Schema constructSchema(String tableName, List<ServiceNowColumn> columns) {
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    List<Schema.Field> fields = schemaBuilder.constructSchemaFields(columns);

    return Schema.recordOf(tableName, fields);
  }

  private List<Schema.Field> constructSchemaFields(List<ServiceNowColumn> columns) {
    return columns.stream()
      .map(o -> transformToField(o))
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  private Schema.Field transformToField(ServiceNowColumn column) {
    String name = column.getFieldName();
    if (Strings.isNullOrEmpty(name)) {
      return null;
    }

    Schema schema = createSchema(column);
    if (schema == null) {
      return null;
    }

    return Schema.Type.NULL == schema.getType()
      ? Schema.Field.of(name, schema)
      : Schema.Field.of(name, Schema.nullableOf(schema));
  }

  private Schema createSchema(ServiceNowColumn column) {
    switch (column.getTypeName().toLowerCase()) {
      case "decimal":
        return Schema.of(Schema.Type.DOUBLE);
      case "integer":
        return Schema.of(Schema.Type.INT);
      case "boolean":
        return Schema.of(Schema.Type.BOOLEAN);
      case "glide_date":
        return Schema.of(Schema.LogicalType.DATE);
      case "glide_date_time":
        return Schema.of(Schema.LogicalType.DATETIME);
      case "glide_time":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case "reference":
      case "currency":
      case "sys_class_name":
      case "domain_id":
      case "domain_path":
      case "guid":
      case "translated_html":
      case "journal":
      case "choice":
      case "string":
      default:
        return Schema.of(Schema.Type.STRING);
    }
  }
}
