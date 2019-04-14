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
package co.cask.hydrator.salesforce.plugin.source.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.hydrator.salesforce.SObjectDescriptor;
import co.cask.hydrator.salesforce.SalesforceConstants;
import co.cask.hydrator.salesforce.SalesforceQueryUtil;
import co.cask.hydrator.salesforce.SalesforceSchemaUtil;
import co.cask.hydrator.salesforce.parser.SOQLParsingException;
import co.cask.hydrator.salesforce.parser.SalesforceQueryParser;
import co.cask.hydrator.salesforce.plugin.BaseSalesforceConfig;
import co.cask.hydrator.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.sforce.ws.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class {@link SalesforceSourceConfig} provides all the configuration required for
 * configuring the {@link SalesforceBatchSource} plugin.
 */
public class SalesforceSourceConfig extends BaseSalesforceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSourceConfig.class);

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

  @Name(SalesforceSourceConstants.PROPERTY_DATETIME_FILTER)
  @Description("Salesforce SObject query datetime filter. Example: 2019-03-12T11:29:52Z, LAST_WEEK")
  @Nullable
  @Macro
  private String datetimeFilter;

  @Name(SalesforceSourceConstants.PROPERTY_DURATION)
  @Description("Salesforce SObject query duration. Default value: 0")
  @Nullable
  @Macro
  private Integer duration;

  @Name(SalesforceSourceConstants.PROPERTY_OFFSET)
  @Description("Salesforce SObject query offset. Default value: 0")
  @Nullable
  @Macro
  private Integer offset;

  @Name(SalesforceSourceConstants.PROPERTY_SCHEMA)
  @Macro
  @Nullable
  @Description("Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.")
  private String schema;

  @VisibleForTesting
  SalesforceSourceConfig(String referenceName,
                                String clientId,
                                String clientSecret,
                                String username,
                                String password,
                                String loginUrl,
                                String errorHandling,
                                @Nullable String query,
                                @Nullable String sObjectName,
                                @Nullable String datetimeFilter,
                                @Nullable Integer duration,
                                @Nullable Integer offset,
                                @Nullable String schema) {
    super(referenceName, clientId, clientSecret, username, password, loginUrl, errorHandling);
    this.query = query;
    this.sObjectName = sObjectName;
    this.datetimeFilter = datetimeFilter;
    this.duration = duration;
    this.offset = offset;
    this.schema = schema;
  }

  /**
   * Returns SOQL to retrieve data from Salesforce. If user has provided SOQL, returns given SOQL.
   * If user has provided sObject name, generates SOQL based on sObject metadata and provided filters.
   *
   * @return SOQL query
   */
  public String getQuery() {
    String soql = isSoqlQuery() ? query : getSObjectQuery();
    return Objects.requireNonNull(soql).trim();
  }

  @Nullable
  public String getSObjectName() {
    return sObjectName;
  }

  public int getDuration() {
    return Objects.isNull(duration) ? SalesforceSourceConstants.DURATION_DEFAULT : duration;
  }

  public int getOffset() {
    return Objects.isNull(offset) ? SalesforceSourceConstants.OFFSET_DEFAULT : offset;
  }

  @Nullable
  public String getDatetimeFilter() {
    return datetimeFilter;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigPropertyException("Unable to parse output schema: " +
        schema, e, SalesforceSourceConstants.PROPERTY_SCHEMA);
    }
  }

  public boolean isSoqlQuery() {
    if (!Strings.isNullOrEmpty(query)) {
      return true;
    } else if (!Strings.isNullOrEmpty(sObjectName)) {
      return false;
    }
    throw new InvalidConfigPropertyException("SOQL query or SObject name must be provided",
                                             SalesforceSourceConstants.PROPERTY_QUERY);
  }

  @Override
  public void validate() {
    super.validate();
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY) && !Strings.isNullOrEmpty(query)) {
      try {
        SalesforceQueryParser.validateQuery(query);
      } catch (SOQLParsingException e) {
        throw new InvalidConfigPropertyException(String.format("Invalid SOQL query: '%s", query), e,
                                                 SalesforceSourceConstants.PROPERTY_QUERY);
      }
    }
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY)
      && !containsMacro(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)) {
      if (!isSoqlQuery()) {
        validateSObjectFilter(SalesforceSourceConstants.PROPERTY_DURATION, getDuration());
        validateSObjectFilter(SalesforceSourceConstants.PROPERTY_OFFSET, getOffset());
      }
    }
    validateSchema();
  }

  private void validateSObjectFilter(String propertyName, int propertyValue) {
    if (!containsMacro(propertyName) && propertyValue < SalesforceConstants.INTERVAL_FILTER_MIN_VALUE) {
      throw new InvalidConfigPropertyException(
        String.format("Invalid SObject '%s' value: '%d'. Value must be '%d' or greater", propertyName, propertyValue,
                      SalesforceConstants.INTERVAL_FILTER_MIN_VALUE), propertyName);
    }
  }

  /**
   * Generates SOQL based on given sObject name metadata and filter properties.
   * Includes only those sObject fields which are present in the schema.
   * This allows to avoid pulling data from Salesforce for the fields which are not needed.
   *
   * @return SOQL generated based on sObject metadata and given filters
   */
  private String getSObjectQuery() {
    try {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName, getAuthenticatorCredentials());

      Schema schema = getSchema();
      List<String> sObjectFields = sObjectDescriptor.getFieldsNames();

      List<String> fieldNames;
      if (schema == null) {
        fieldNames = sObjectFields;
      } else {
        fieldNames = sObjectFields.stream()
          .filter(name -> schema.getField(name) != null)
          .collect(Collectors.toList());

        if (fieldNames.isEmpty()) {
          throw new IllegalArgumentException(
            String.format("None of the fields indicated in schema are present in sObject metadata."
              + " Schema: '%s'. SObject fields: '%s'", schema, sObjectFields));
        }
      }

      String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fieldNames, sObjectName,
                                                                   getDuration(), getOffset(), datetimeFilter);
      LOG.debug("Generated SObject query: '{}'", sObjectQuery);
      return sObjectQuery;
    } catch (ConnectionException e) {
      throw new IllegalStateException(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s'", sObjectName), e);
    }
  }

  private void validateSchema() {
    if (containsMacro(SalesforceSourceConstants.PROPERTY_SCHEMA)) {
      return;
    }

    Schema schema = getSchema();
    if (schema != null) {
      SalesforceSchemaUtil.validateFieldSchemas(schema);
    }
  }
}
