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
package io.cdap.plugin.salesforce;

import io.cdap.cdap.api.data.schema.Schema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * Utility class that handles various value transformations based on given schema type.
 */
public class SalesforceTransformUtil {

  /**
   * Transforms given value based on the given logical type.
   *
   * @param fieldName field name
   * @param logicalType logical type
   * @param value field value in string representation
   * @return transformed value
   */
  public static Object transformLogicalType(String fieldName, Schema.LogicalType logicalType, String value) {
    switch (logicalType) {
      case DATE:
        // date will be in yyyy-mm-dd format
        return Math.toIntExact(LocalDate.parse(value).toEpochDay());
      case TIMESTAMP_MICROS:
        return TimeUnit.MILLISECONDS.toMicros(Instant.parse(value).toEpochMilli());
      case TIME_MICROS:
        return TimeUnit.NANOSECONDS.toMicros(LocalTime.parse(value).toNanoOfDay());
      default:
        throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
    }
  }

  /**
   * Convert a schema field to String which can be read by Salesforce.
   *
   * @param value field value
   * @param field schema field
   * @return string representing the value in format, which can be understood by Salesforce
   */
  public static String convertSchemaFieldToString(Object value, Schema.Field field) {
    // don't convert null to avoid NPE
    if (value == null) {
      return null;
    }

    Schema fieldSchema = field.getSchema();

    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      Instant instant;
      switch (logicalType) {
        case DATE:
          // convert epoch day to yyyy-mm-dd format
          return LocalDate.ofEpochDay((Integer) value).toString();
        case TIMESTAMP_MICROS:
          // convert timestamp to ISO 8601 format
          instant = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis((Long) value));
          return instant.toString();
        case TIME_MICROS:
          // convert timestamp to HH:mm:ss,SSS
          instant = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis((Long) value));
          return instant.atZone(ZoneOffset.UTC).toLocalTime().toString();
        case TIMESTAMP_MILLIS:
          // convert timestamp to ISO 8601 format
          instant = Instant.ofEpochMilli((Long) value);
          return instant.toString();
        case TIME_MILLIS:
          // convert timestamp to HH:mm:ss,SSS
          instant = Instant.ofEpochMilli((Long) value);
          return instant.atZone(ZoneOffset.UTC).toLocalTime().toString();
        default:
          throw new IllegalArgumentException(
            String.format("Field '%s' is of unsupported type '%s'", field.getName(), logicalType.getToken()));
      }
    }

    return value.toString();
  }
}
