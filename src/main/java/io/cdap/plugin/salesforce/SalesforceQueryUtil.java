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

import io.cdap.plugin.salesforce.parser.SalesforceQueryParser;

import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Provides Salesforce query utility methods.
 */
public class SalesforceQueryUtil {
  private static final String LESS_THAN = "<";
  private static final String GREATER_THAN_OR_EQUAL = ">=";

  private static final String SELECT = "SELECT ";
  private static final String FROM = " FROM ";
  private static final String WHERE = " WHERE ";
  private static final String AND = " AND ";


  private static final String FIELD_LAST_MODIFIED_DATE = "LastModifiedDate";
  private static final String FIELD_ID = "Id";

  /**
   * Creates SObject query with filter if provided.
   *
   * @param fields           query fields
   * @param sObjectName      SObject name
   * @param filterDescriptor SObject date filter descriptor
   * @return SOQL with filter if SObjectFilterDescriptor type is not NoOp
   */
  public static String createSObjectQuery(List<String> fields, String sObjectName,
                                          SObjectFilterDescriptor filterDescriptor) {
    StringBuilder query = new StringBuilder()
      .append(SELECT).append(String.join(",", fields))
      .append(FROM).append(sObjectName);


    if (!filterDescriptor.isNoOp()) {
      query.append(WHERE).append(generateSObjectFilter(filterDescriptor));
    }
    return query.toString();
  }

  /**
   * Checks if query length is less than SOQL max length limit.
   *
   * @param query SOQL query
   * @return true if SOQL length less than max allowed
   */
  public static boolean isQueryUnderLengthLimit(String query) {
    return query.length() < SalesforceConstants.SOQL_MAX_LENGTH;
  }

  /**
   * Creates SObject IDs query based on initial query. Replaces all query fields with {@link #FIELD_ID} in SELECT
   * clause but leaves other clauses as is.
   * <p/>
   * Example:
   * <ul>
   *  <li>Initial query: `SELECT Name, LastModifiedDate FROM Opportunity WHERE Name LIKE 'S_%'`</li>
   *  <li>Result query: `SELECT Id FROM Opportunity WHERE Name LIKE 'S_%'`</li>
   * </ul>
   *
   * @param query initial query
   * @return SObject IDs query
   */
  public static String createSObjectIdQuery(String query) {
    String fromStatement = SalesforceQueryParser.getFromStatement(query);
    return SELECT + FIELD_ID + " " + fromStatement;
  }

  /**
   * Generates SObject query filter based on provided values.
   *
   * @param filterDescriptor filter options holder
   * @return datetime, range or no-op filter
   */
  private static String generateSObjectFilter(SObjectFilterDescriptor filterDescriptor) {
    StringBuilder filter = new StringBuilder();
    if (filterDescriptor.getStartTime() != null) {
      filter.append(FIELD_LAST_MODIFIED_DATE)
        .append(GREATER_THAN_OR_EQUAL)
        .append(filterDescriptor.getStartTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }
    if (filterDescriptor.getEndTime() != null) {
      if (filter.length() > 0) {
        filter.append(AND);
      }
      filter.append(FIELD_LAST_MODIFIED_DATE)
        .append(LESS_THAN)
        .append(filterDescriptor.getEndTime().format(DateTimeFormatter.ISO_DATE_TIME));
    }
    return filter.toString();
  }
}
