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

package co.cask.hydrator.salesforce.plugin.source.batch.util;

import com.google.common.base.Strings;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Provides Salesforce query utility methods.
 */
public class SalesforceQueryUtil {
  private static final String NO_OP_FILTER = "";
  private static final String LAST_MODIFIED_DATE = "LastModifiedDate";
  private static final String LESS_THAN = "<";
  private static final String GREATER_THAN = ">";
  private static final String SELECT = "SELECT ";
  private static final String FROM = " FROM ";
  private static final String WHERE = " WHERE ";
  private static final String AND = " AND ";

  /**
   * Creates SObject query with filter if provided. Uses current datetime in UTC timezone for range filter
   * by default.
   *
   * @param fields         query fields
   * @param sObjectName    SObject name
   * @param duration       SObject query duration
   * @param offset         SObject query offset
   * @param datetimeFilter SObject query datetime filter
   * @return SOQL with filter if provided
   */
  public static String createSObjectQuery(List<String> fields, String sObjectName, long duration,
                                          long offset, String datetimeFilter) {
    return createSObjectQuery(fields, sObjectName, ZonedDateTime.now(ZoneOffset.UTC), duration, offset, datetimeFilter);
  }

  /**
   * Creates SObject query with filter if provided.
   *
   * @param fields         query fields
   * @param sObjectName    SObject name
   * @param dateTime       application start time
   * @param duration       SObject query duration
   * @param offset         SObject query offset
   * @param datetimeFilter SObject query datetime filter
   * @return SOQL with filter if provided
   */
  public static String createSObjectQuery(List<String> fields, String sObjectName, ZonedDateTime dateTime,
                                          long duration, long offset, String datetimeFilter) {
    StringBuilder query = new StringBuilder()
      .append(SELECT).append(String.join(",", fields))
      .append(FROM).append(sObjectName);

    String sObjectFilter = generateSObjectFilter(dateTime, duration, offset, datetimeFilter);
    if (!Strings.isNullOrEmpty(sObjectFilter)) {
      query.append(WHERE).append(sObjectFilter);
    }
    return query.toString();
  }

  /**
   * Generates SObject query filter based on provided values.
   *
   * @param dateTime       application start time
   * @param duration       SObject query duration
   * @param offset         SObject query offset
   * @param datetimeFilter SObject query datetime filter
   * @return interval or datetime filter based on provided values
   */
  private static String generateSObjectFilter(ZonedDateTime dateTime, long duration, long offset,
                                              String datetimeFilter) {
    return Strings.isNullOrEmpty(datetimeFilter) || datetimeFilter.trim().isEmpty()
      ? generateRangeFilter(LAST_MODIFIED_DATE, dateTime, duration, offset)
      : generateIncrementalFilter(LAST_MODIFIED_DATE, datetimeFilter);
  }

  /**
   * Generates range date filter based on duration and offset.
   * <p>
   * Examples:
   * <ul>
   * <li>if duration is '6' (6 hours) and the pipeline runs at 9am, it will read data updated
   * from 3am - 9am.</li>
   *
   * <li>if duration is '6' and the offset is '1' and the pipeline runs at 9am, it will read
   * data updated from 2am - 8am.</li>
   * </ul>
   *
   * @param fieldName dateTime field name
   * @param dateTime  pipeline start time
   * @param duration  pipeline fetch duration
   * @param offset    offset from pipeline start time
   * @return datetime filter or empty string if duration is not set
   */
  private static String generateRangeFilter(String fieldName, ZonedDateTime dateTime, long duration,
                                            long offset) {
    if (duration <= SalesforceSourceConstants.INTERVAL_FILTER_MIN_VALUE) {
      // no filter is required
      return NO_OP_FILTER;
    }
    ZonedDateTime endTime = dateTime.minusHours(offset);
    ZonedDateTime startTime = endTime.minusHours(duration);
    return fieldName + GREATER_THAN + startTime.format(DateTimeFormatter.ISO_DATE_TIME) +
      AND +
      fieldName + LESS_THAN + endTime.format(DateTimeFormatter.ISO_DATE_TIME);
  }

  /**
   * Generates incremental date filter based on provided datetimeValue.
   *
   * @param datetimeValue dateTime in ISO format or SOQL date literal
   * @return datetime filter or empty string if datetimeValue is blank
   */
  private static String generateIncrementalFilter(String fieldName, String datetimeValue) {
    // extract first value from datetimeValue to avoid SQL injection. Delimiter is space.
    // Example: `   2018-01-01 OR Id LIKE '%'  ` replaced with `2018-01-01`
    String firstValue = datetimeValue.trim().split(" ", 2)[0];
    return fieldName + GREATER_THAN + firstValue;
  }
}
