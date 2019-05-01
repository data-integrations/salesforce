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

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.plugin.BaseSalesforceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Base Salesforce Batch Source config. Contains common configuration properties and methods.
 */
public abstract class SalesforceBaseSourceConfig extends BaseSalesforceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBaseSourceConfig.class);

  @Name(SalesforceSourceConstants.PROPERTY_DATETIME_FILTER)
  @Description("Salesforce SObject query datetime filter. Example: 2019-03-12T11:29:52Z, LAST_WEEK")
  @Nullable
  @Macro
  private String datetimeFilter;

  @Name(SalesforceSourceConstants.PROPERTY_DURATION)
  @Description("Salesforce SObject query duration. Time unit: hours. Default value: 0")
  @Nullable
  @Macro
  private Integer duration;

  @Name(SalesforceSourceConstants.PROPERTY_OFFSET)
  @Description("Salesforce SObject query offset. Time unit: hours. Default value: 0")
  @Nullable
  @Macro
  private Integer offset;

  protected SalesforceBaseSourceConfig(String referenceName,
                                       String clientId,
                                       String clientSecret,
                                       String username,
                                       String password,
                                       String loginUrl,
                                       String errorHandling,
                                       @Nullable String datetimeFilter,
                                       @Nullable Integer duration,
                                       @Nullable Integer offset) {
    super(referenceName, clientId, clientSecret, username, password, loginUrl, errorHandling);
    this.datetimeFilter = datetimeFilter;
    this.duration = duration;
    this.offset = offset;
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

  protected void validateFilters() {
    validateSObjectFilter(SalesforceSourceConstants.PROPERTY_DURATION, getDuration());
    validateSObjectFilter(SalesforceSourceConstants.PROPERTY_OFFSET, getOffset());
  }

  /**
   * Generates SOQL based on given sObject name metadata and filter properties.
   * Includes only those sObject fields which are present in the schema.
   * This allows to avoid pulling data from Salesforce for the fields which are not needed.
   *
   * @param sObjectName Salesforce object name
   * @param schema CDAP schema
   *
   * @return SOQL generated based on sObject metadata and given filters
   */
  protected String getSObjectQuery(String sObjectName, Schema schema) {
    try {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName, getAuthenticatorCredentials());

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
        getDuration(), getOffset(), getDatetimeFilter());
      LOG.debug("Generated SObject query: '{}'", sObjectQuery);
      return sObjectQuery;
    } catch (ConnectionException e) {
      throw new IllegalStateException(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s'", sObjectName), e);
    }
  }

  private void validateSObjectFilter(String propertyName, int propertyValue) {
    if (!containsMacro(propertyName) && propertyValue < SalesforceConstants.INTERVAL_FILTER_MIN_VALUE) {
      throw new InvalidConfigPropertyException(
        String.format("Invalid SObject '%s' value: '%d'. Value must be '%d' or greater", propertyName, propertyValue,
          SalesforceConstants.INTERVAL_FILTER_MIN_VALUE), propertyName);
    }
  }
}
