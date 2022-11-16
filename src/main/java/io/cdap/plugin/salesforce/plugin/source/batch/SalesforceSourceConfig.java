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
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectsDescribeResult;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
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
 * This class {@link SalesforceSourceConfig} provides all the configuration required for configuring the {@link
 * SalesforceBatchSource} plugin.
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

  @Name(SalesforceSourceConstants.PROPERTY_PK_CHUNK_ENABLE_NAME)
  @Macro
  @Nullable
  @Description("Primary key (PK) Chunking splits query on large tables into chunks based on the record IDs, or " +
    "primary keys, of the queried records.")
  private Boolean enablePKChunk;

  @Name(SalesforceSourceConstants.PROPERTY_CHUNK_SIZE_NAME)
  @Macro
  @Nullable
  @Description("Specify size of chunk. Maximum Size is 250,000. Default Size is 100,000.")
  private Integer chunkSize;

  @Name(SalesforceSourceConstants.PROPERTY_PARENT_NAME)
  @Macro
  @Nullable
  @Description("Parent of the Salesforce Object. This is used to enable chunking for history tables or shared objects.")
  private String parent;

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
                         @Nullable String operation,
                         @Nullable OAuthInfo oAuthInfo,
                         @Nullable Boolean enablePKChunk,
                         @Nullable Integer chunkSize,
                         @Nullable String parent) {
    super(referenceName, consumerKey, consumerSecret, username, password, loginUrl,
          datetimeAfter, datetimeBefore, duration, offset, securityToken, oAuthInfo, operation);
    this.query = query;
    this.sObjectName = sObjectName;
    this.schema = schema;
    this.enablePKChunk = enablePKChunk;
    this.chunkSize = chunkSize;
    this.parent = parent;
  }


  /**
   * Returns SOQL to retrieve data from Salesforce. If user has provided SOQL, returns given SOQL. If user has provided
   * sObject name, generates SOQL based on sObject metadata and provided filters.
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

  public String getParent() {
    // Return empty string instead of null to avoid error when passing this value as a Configuration
    // in SalesforceInputFormat
    return parent == null ? "" : parent;
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

  public void validate(FailureCollector collector) {
    this.getConnection().validate(collector);
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
      if (getConnection().canAttemptToEstablishConnection()) {
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
    validatePKChunk(collector);
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
    if (getConnection() != null) {
      try {
        SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName,
                                                                         getConnection().getAuthenticatorCredentials());
        List<String> compoundFieldNames = sObjectDescriptor.getFields().stream()
          .filter(fieldDescriptor -> fieldNames.contains(fieldDescriptor.getName()))
          .filter(fieldDescriptor -> SalesforceSchemaUtil.COMPOUND_FIELDS.contains(fieldDescriptor.getFieldType()))
          .map(SObjectDescriptor.FieldDescriptor::getName)
          .collect(Collectors.toList());
        if (!compoundFieldNames.isEmpty()) {
          collector.addFailure(
              String.format("Compound fields %s cannot be fetched when a SOQL query is given. "
                              + "Please specify the individual attributes instead of compound field name in SOQL " +
                              "query. "
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
  private void validatePKChunk(FailureCollector collector) {
    if (containsMacro(SalesforceSourceConstants.PROPERTY_PK_CHUNK_ENABLE_NAME)
      || containsMacro(SalesforceSourceConstants.PROPERTY_CHUNK_SIZE_NAME)
      || containsMacro(SalesforceSourceConstants.PROPERTY_PARENT_NAME)) {
      return;
    }

    if (!getEnablePKChunk()) {
      return;
    }

    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY) && !Strings.isNullOrEmpty(query)) {
      if (SalesforceQueryParser.isRestrictedPKQuery(query)) {
        collector.addFailure(
          String.format("SOQL Query contains restricted clauses when PK Chunk is Enabled. Unsupported query: '%s'.",
                        query),
          "Set Enable PK Chunk to false, because 'WHERE' is the only supported conditions clause.")
          .withConfigProperty(SalesforceSourceConstants.PROPERTY_QUERY);
      }

      // If a parent object is defined then use that to check for PK support, otherwise use the object from the query
      if (!containsMacro(SalesforceSourceConstants.PROPERTY_PARENT_NAME) && !Strings.isNullOrEmpty(getParent())) {
        checkForPKSupportedObject(getParent(), collector);
      } else {
        String sObject = SalesforceQueryParser.getObjectDescriptorFromQuery(query).getName();
        checkForPKSupportedObject(sObject, collector);
      }
    }

    // If a parent object is defined then use that to check for PK support, otherwise use the object from the config
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME) && !Strings.isNullOrEmpty(getSObjectName())) {
      if (!containsMacro(SalesforceSourceConstants.PROPERTY_PARENT_NAME) && !Strings.isNullOrEmpty(getParent())) {
        checkForPKSupportedObject(getParent(), collector);
      } else {
        checkForPKSupportedObject(getSObjectName(), collector);
      }
    }

    if (getChunkSize() > SalesforceSourceConstants.MAX_PK_CHUNK_SIZE) {
      collector.addFailure(
        String.format("Chunk Size '%d' is bigger than maximum allowed size '%d'.", getChunkSize(),
                      SalesforceSourceConstants.MAX_PK_CHUNK_SIZE),
        String.format("Allowed values are between the range of '%d' and '%d'.",
                      SalesforceSourceConstants.MIN_PK_CHUNK_SIZE, SalesforceSourceConstants.MAX_PK_CHUNK_SIZE))
        .withConfigProperty(SalesforceSourceConstants.PROPERTY_CHUNK_SIZE_NAME);
    } else if (getChunkSize() < SalesforceSourceConstants.MIN_PK_CHUNK_SIZE) {
      collector.addFailure(
        String.format("Chunk Size %d is lower than minimum allowed size '%d'.", getChunkSize(),
                      SalesforceSourceConstants.MIN_PK_CHUNK_SIZE),
        String.format("Allowed values are between the range of '%d' and '%d'.",
                      SalesforceSourceConstants.MIN_PK_CHUNK_SIZE, SalesforceSourceConstants.MAX_PK_CHUNK_SIZE))
        .withConfigProperty(SalesforceSourceConstants.PROPERTY_CHUNK_SIZE_NAME);
    }
  }

  private void checkForPKSupportedObject(String sObject, FailureCollector collector) {
    if (getConnection() != null) {
      if (getConnection().canAttemptToEstablishConnection()) {
        if (!isCustomObject(sObject, collector)) {
          if (!SUPPORTED_OBJECTS_WITH_PK_CHUNK.contains(sObject)) {
            collector.addFailure(String.format("SObject '%s' is not supported with PKChunk enabled.", sObject),
                                 "Please check documentation for supported Objects. " +
                                   "If this is a history " +
                                   "or shared table, you may need to specify a SObject Parent in the" +
                                   " Advanced section.");
          }
        }
      }
    }
  }

  public boolean getEnablePKChunk() {
    return enablePKChunk == null ? false : enablePKChunk;
  }

  public int getChunkSize() {
    return chunkSize == null ? SalesforceSourceConstants.DEFAULT_PK_CHUNK_SIZE : chunkSize;
  }

  private boolean isCustomObject(String sObjectName, FailureCollector collector) {
    AuthenticatorCredentials credentials = this.getConnection().getAuthenticatorCredentials();
    try {
      PartnerConnection partnerConnection = new PartnerConnection(Authenticator.createConnectorConfig(credentials));
      return SObjectsDescribeResult.isCustomObject(partnerConnection, sObjectName);
    } catch (ConnectionException e) {
      collector.addFailure("There was issue communicating with Salesforce", null).withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }
  }
}
