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
package io.cdap.plugin.servicenow.connector;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.text.NumberFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for converting the record from ServiceNow data type to CDAP schema data types
 */
public class ServiceNowRecordConverter {

  // supported date and time formats.
  // ref: https://www.servicenow.com/docs/bundle/yokohama-api-reference/page/app-store/
  // dev_portal/API_reference/GlideDateTime/concept/c_GlideDateTimeAPI.html
  // Instead of using DateTimeFormatterBuilder.appendOptional(...), we maintain a List<DateTimeFormatter>
  // and attempt to parse the input with each formatter in order. This approach is more reliable because
  // DateTimeFormatterBuilder processes patterns sequentially and may partially match an incorrect format.
  // For example, if "MM/dd/yyyy HH:mm:ss" is tried before "dd/MM/yyyy HH:mm:ss", an input like
  // "14/06/2025 13:00:00" would fail, as 14 is not a valid month.
  // By explicitly trying each formatter, we avoid such ambiguities and ensure correct parsing of
  // date formats with overlapping patterns.
  private static final List<DateTimeFormatter> DATE_TIME_FORMATTER = Collections.unmodifiableList(
      Arrays.asList(
        DateTimeFormatter.ofPattern("dd.MM.yyyy hh:mm:ss a"),
        DateTimeFormatter.ofPattern("dd.MM.yyyy hh.mm.ss a"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("dd.MM.yyyy HH.mm.ss"),
        DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("dd-MM-yyyy HH.mm.ss"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("dd-MM-yy HH.mm.ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
        DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm"),
        DateTimeFormatter.ofPattern("dd-MM-yyyy HH.mm")
      ));

  private static final List<DateTimeFormatter> DATE_FORMATTER = Collections.unmodifiableList(
    Arrays.asList(
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("dd.MM.yyyy"),
      DateTimeFormatter.ofPattern("dd-MM-yyyy"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("MM-dd-yyyy")
    ));

  private static final List<DateTimeFormatter> TIME_FORMATTER = Collections.unmodifiableList(
    Arrays.asList(
      DateTimeFormatter.ofPattern("HH:mm:ss"),
      DateTimeFormatter.ofPattern("HH:mm")
    ));

  public static void convertToValue(String fieldName, Schema fieldSchema, Map<String, String> record,
                                    StructuredRecord.Builder recordBuilder) {
    String fieldValue = record.get(fieldName);
    if (fieldValue == null || fieldValue.isEmpty()) {
      // Set 'null' value as it is
      recordBuilder.set(fieldName, null);
      return;
    }

    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
    // Get values of logical types properly
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATETIME:
          recordBuilder.setDateTime(fieldName,
            parseDateTimeFormat(fieldValue, fieldName, fieldSchema.getDisplayName()));
          return;
        case DATE:
          recordBuilder.setDate(fieldName,
            parseDateFormat(fieldValue, fieldName, fieldSchema.getDisplayName()));
          return;
        case TIME_MICROS:
          recordBuilder.setTime(fieldName,
            parseTimeFormat(fieldValue, fieldName, fieldSchema.getDisplayName()));
          return;
        default:
          throw new IllegalStateException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        recordBuilder.set(fieldName, fieldValue);
        return;
      case DOUBLE:
        recordBuilder.set(fieldName, convertToDoubleValue(fieldValue));
        return;
      case INT:
        recordBuilder.set(fieldName, convertToIntegerValue(fieldValue));
        return;
      case BOOLEAN:
        recordBuilder.set(fieldName, convertToBooleanValue(fieldValue));
        return;
      default:
        throw new IllegalStateException(
          String.format("Record type '%s' is not supported for field '%s'", fieldType.name(), fieldName));
    }

  }

  @VisibleForTesting
  public static Double convertToDoubleValue(String fieldValue) {
    try {
      return NumberFormat.getNumberInstance(Locale.US).parse(fieldValue).doubleValue();
    } catch (ParseException exception) {
      throw new UnexpectedFormatException(
        String.format("Field with value '%s' is not in valid format.", fieldValue), exception);
    }
  }

  @VisibleForTesting
  public static Integer convertToIntegerValue(String fieldValue) {
    try {
      return NumberFormat.getNumberInstance(java.util.Locale.US).parse(fieldValue).intValue();
    } catch (ParseException exception) {
      throw new UnexpectedFormatException(
        String.format("Field with value '%s' is not in valid format.", fieldValue), exception);
    }
  }

  @VisibleForTesting
  public static Boolean convertToBooleanValue(String fieldValue) {
    if (fieldValue.equalsIgnoreCase(Boolean.TRUE.toString()) ||
                                      fieldValue.equalsIgnoreCase(Boolean.FALSE.toString())) {
      return Boolean.parseBoolean(fieldValue);
    }
    throw new UnexpectedFormatException(
      String.format("Field with value '%s' is not in valid format.", fieldValue));
    
  }

  private static LocalDateTime parseDateTimeFormat(String fieldValue, String fieldName, String displayName) {
    for (DateTimeFormatter dateTimeFormatter : DATE_TIME_FORMATTER) {
      try {
        return LocalDateTime.parse(fieldValue, dateTimeFormatter);
      } catch (DateTimeParseException e) {
        // Only throw exception once all the formats are checked.
      }
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
        fieldName, displayName, fieldValue));
  }

  private static LocalDate parseDateFormat(String fieldValue, String fieldName, String displayName) {
    for (DateTimeFormatter dateFormatter : DATE_FORMATTER) {
      try {
        return LocalDate.parse(fieldValue, dateFormatter);
      } catch (DateTimeParseException e) {
        // Only throw exception once all the formats are checked.
      }
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
        fieldName, displayName, fieldValue));
  }

  private static LocalTime parseTimeFormat(String fieldValue, String fieldName, String displayName) {
    for (DateTimeFormatter timeFormatter : TIME_FORMATTER) {
      try {
        return LocalTime.parse(fieldValue, timeFormatter);
      } catch (DateTimeParseException e) {
        // Only throw exception once all the formats are checked.
      }
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
        fieldName, displayName, fieldValue));
  }
}
