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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.parser.SOQLParsingException;
import io.cdap.plugin.salesforce.parser.SalesforceQueryParser;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class {@link SalesforceSourceConfig} provides all the configuration required for
 * configuring the {@link SalesforceBatchSource} plugin.
 */
public class SalesforceSourceConfig extends SalesforceBaseSourceConfig {

  @Name(SalesforceSourceConstants.PROPERTY_QUERY)
  @Description("The SOQL query to retrieve results from. Example: select Id, Name from Opportunity")
  @Nullable
  @Macro
  private String query;

  @Name(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)
  @Description("Salesforce SObject name. Example: Opportunity")
  @Nullable
  @Macro
  private String sObjectName;

  @Name(SalesforceSourceConstants.PROPERTY_SCHEMA)
  @Macro
  @Nullable
  @Description("Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.")
  private String schema;

  @VisibleForTesting
  SalesforceSourceConfig(String referenceName,
                         @Nullable String consumerKey,
                         @Nullable String consumerSecret,
                         @Nullable String username,
                         @Nullable String password,
                         @Nullable String loginUrl,
                         @Nullable String query,
                         @Nullable String sObjectName,
                         @Nullable String datetimeAfter,
                         @Nullable String datetimeBefore,
                         @Nullable String duration,
                         @Nullable String offset,
                         @Nullable String schema,
                         @Nullable String securityToken,
                         @Nullable OAuthInfo oAuthInfo) {
    super(referenceName, consumerKey, consumerSecret, username, password, loginUrl,
          datetimeAfter, datetimeBefore, duration, offset, securityToken, oAuthInfo);
    this.query = query;
    this.sObjectName = sObjectName;
    this.schema = schema;
  }

  /**
   * Returns SOQL to retrieve data from Salesforce. If user has provided SOQL, returns given SOQL.
   * If user has provided sObject name, generates SOQL based on sObject metadata and provided filters.
   *
   * @param logicalStartTime application start time
   * @return SOQL query
   */
  public String getQuery(long logicalStartTime) {
    String soql = isSoqlQuery() ? query : getSObjectQuery(sObjectName, getSchema(), logicalStartTime);
    return Objects.requireNonNull(soql).trim();
  }

  @Nullable
  public String getSObjectName() {
    return sObjectName;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigException("Unable to parse output schema: " +
        schema, e, SalesforceSourceConstants.PROPERTY_SCHEMA);
    }
  }

  public boolean isSoqlQuery() {
    if (!Strings.isNullOrEmpty(query)) {
      return true;
    } else if (!Strings.isNullOrEmpty(sObjectName)) {
      return false;
    }
    throw new InvalidConfigException("SOQL query or SObject name must be provided",
                                             SalesforceSourceConstants.PROPERTY_QUERY);
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY) && !Strings.isNullOrEmpty(query)) {
      if (!SalesforceQueryUtil.isQueryUnderLengthLimit(query) && SalesforceQueryParser.isRestrictedQuery(query)) {
        collector.addFailure(
          String.format(
            "SOQL Query with restricted field types (function calls, sub-query fields) or "
              + "GROUP BY [ROLLUP / CUBE], OFFSET clauses cannot exceed SOQL query length: '%d'. "
              + "Unsupported SOQL query: '%s'", SalesforceConstants.SOQL_MAX_LENGTH, query), null)
          .withConfigProperty(SalesforceSourceConstants.PROPERTY_QUERY);
        throw collector.getOrThrowException();
      }
      SObjectDescriptor queryDescriptor;
      try {
        queryDescriptor = SalesforceQueryParser.getObjectDescriptorFromQuery(query);
      } catch (SOQLParsingException e) {
        collector.addFailure(String.format("Invalid SOQL query '%s' : %s", query, e.getMessage()), null)
          .withStacktrace(e.getStackTrace())
          .withConfigProperty(SalesforceSourceConstants.PROPERTY_QUERY);
        throw collector.getOrThrowException();
      }
      if (canAttemptToEstablishConnection()) {
        validateCompoundFields(queryDescriptor.getName(), queryDescriptor.getFieldsNames(), collector);
      }
    }
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY)
      && !containsMacro(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)) {
      try {
        boolean isSoql = isSoqlQuery();
        if (!isSoql) {
          validateFilters(collector);
        }
      } catch (InvalidConfigException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
      }
    }
    validateSchema(collector);
  }

  private void validateSchema(FailureCollector collector) {
    if (containsMacro(SalesforceSourceConstants.PROPERTY_SCHEMA)) {
      return;
    }

    try {
      Schema schema = getSchema();
      if (schema != null) {
        SalesforceSchemaUtil.validateFieldSchemas(schema, collector);
      }
    } catch (InvalidConfigException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
    }
  }

  private void validateCompoundFields(String sObjectName, List<String> fieldNames, FailureCollector collector) {
    try {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName,
                                                                       getAuthenticatorCredentials());
      List<String> compoundFieldNames = sObjectDescriptor.getFields().stream()
        .filter(fieldDescriptor -> fieldNames.contains(fieldDescriptor.getName()))
        .filter(fieldDescriptor -> SalesforceSchemaUtil.COMPOUND_FIELDS.contains(fieldDescriptor.getFieldType()))
        .map(SObjectDescriptor.FieldDescriptor::getName)
        .collect(Collectors.toList());
      if (!compoundFieldNames.isEmpty()) {
        collector.addFailure(
          String.format("Compound fields %s cannot be fetched when a SOQL query is given. "
                          + "Please specify the individual attributes instead of compound field name in SOQL query. "
                          + "For example, instead of 'Select BillingAddress ...', use "
                          + "'Select BillingCountry, BillingCity, BillingStreet ...'",
                        compoundFieldNames), null)
          .withConfigProperty(SalesforceSourceConstants.PROPERTY_QUERY);
      }
    } catch (ConnectionException e) {
      collector.addFailure(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s'", sObjectName), null)
        .withStacktrace(e.getStackTrace());
    }
  }
}
