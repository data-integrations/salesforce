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
}
