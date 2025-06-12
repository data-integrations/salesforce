/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.servicenow.sink.model;

import java.util.Map;

/**
 * Model class for Schema Result returned by the ServiceNow Column Schema API.
 *
 * <p>The {@code columns} map contains metadata for each column in the ServiceNow table.
 * The key of the map is the column's internal name (as used in the table schema),
 * and the value is a {@link ServiceNowSchemaField} object containing the details for that column.
 *
 * <p>Example JSON from ServiceNow:
 * <pre>
 * {
 *   "result": {
 *     "columns": {
 *       "state": {
 *         "label": "State",
 *         "type": "string",
 *         "internal_type": "integer",
 *         "name": "state"
 *       },
 *       "active": {
 *         "label": "Active",
 *         "type": "boolean",
 *         "internal_type": "boolean",
 *         "name": "active"
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * In this example, the map will contain keys like {@code "state"} and {@code "active"},
 * each pointing to a {@code ServiceNowSchemaField} instance with metadata about that field.
 */
public class ServiceNowSchemaResult {
  private final Map<String, ServiceNowSchemaField> columns;

  public ServiceNowSchemaResult(Map<String, ServiceNowSchemaField> columns) {
    this.columns = columns;
  }

  public Map<String, ServiceNowSchemaField> getColumns() {
    return columns;
  }
}
