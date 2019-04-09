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

package io.cdap.salesforce.plugin.source.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.salesforce.SObjectDescriptor;
import io.cdap.salesforce.parser.SOQLParsingException;
import io.cdap.salesforce.parser.SalesforceQueryParser;
import io.cdap.salesforce.plugin.BaseSalesforceConfig;
import io.cdap.salesforce.plugin.source.batch.util.SalesforceQueryUtil;
import io.cdap.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
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
                                @Nullable Integer offset) {
    super(referenceName, clientId, clientSecret, username, password, loginUrl, errorHandling);
    this.query = query;
    this.sObjectName = sObjectName;
    this.datetimeFilter = datetimeFilter;
    this.duration = duration;
    this.offset = offset;
  }

  public String getQuery() {
    if (isSoqlQuery()) {
      return query;
    }
    return getSObjectQuery();
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
  }

  private void validateSObjectFilter(String propertyName, int propertyValue) {
    if (!containsMacro(propertyName) && propertyValue < SalesforceSourceConstants.INTERVAL_FILTER_MIN_VALUE) {
      throw new InvalidConfigPropertyException(
        String.format("Invalid SObject '%s' value: '%d'. Value must be '%d' or greater", propertyName, propertyValue,
                   SalesforceSourceConstants.INTERVAL_FILTER_MIN_VALUE), propertyName);
    }
  }

  private String getSObjectQuery() {
    try {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName, getAuthenticatorCredentials());
      String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(sObjectDescriptor.getFieldsNames(), sObjectName,
                                                                   getDuration(), getOffset(), datetimeFilter);
      LOG.debug("Generated SObject query: '{}'", sObjectQuery);
      return sObjectQuery;
    } catch (ConnectionException e) {
      throw new IllegalStateException(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s'", sObjectName), e);
    }
  }
}
