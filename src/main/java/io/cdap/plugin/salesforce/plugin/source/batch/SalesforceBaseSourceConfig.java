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

import com.google.common.base.Strings;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectFilterDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Base Salesforce Batch Source config. Contains common configuration properties and methods.
 */
public abstract class SalesforceBaseSourceConfig extends ReferencePluginConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBaseSourceConfig.class);
  private static final String DEFAULT_OPERATION = "query";
  private static final String DEFAULT_LOGIN_URL = "https://login.salesforce.com/services/oauth2/token";
  @Name(SalesforceSourceConstants.PROPERTY_DATETIME_AFTER)
  @Description("Salesforce SObject query datetime filter. Example: 2019-03-12T11:29:52Z")
  @Nullable
  @Macro
  private String datetimeAfter;

  @Name(SalesforceSourceConstants.PROPERTY_DATETIME_BEFORE)
  @Description("Salesforce SObject query datetime filter. Example: 2019-03-12T11:29:52Z")
  @Nullable
  @Macro
  private String datetimeBefore;

  @Name(SalesforceSourceConstants.PROPERTY_DURATION)
  @Description("Salesforce SObject query duration.")
  @Nullable
  @Macro
  private String duration;

  @Name(SalesforceSourceConstants.PROPERTY_OFFSET)
  @Description("Salesforce SObject query offset.")
  @Nullable
  @Macro
  private String offset;

  @Name(SalesforceSourceConstants.PROPERTY_OPERATION)
  @Description("If set to query, the query result will only return current rows. If set to queryAll, " +
    "all records, including deletes will be sourced")
  @Nullable
  private String operation;

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private SalesforceConnectorConfig connection;

  protected SalesforceBaseSourceConfig(String referenceName,
                                       @Nullable String consumerKey,
                                       @Nullable String consumerSecret,
                                       @Nullable String username,
                                       @Nullable String password,
                                       @Nullable String loginUrl,
                                       @Nullable Integer connectTimeout,
                                       @Nullable String datetimeAfter,
                                       @Nullable String datetimeBefore,
                                       @Nullable String duration,
                                       @Nullable String offset,
                                       @Nullable String securityToken,
                                       @Nullable OAuthInfo oAuthInfo,
                                       @Nullable String operation,
                                       @Nullable String proxyUrl) {
    super(referenceName);
    this.connection = new SalesforceConnectorConfig(consumerKey, consumerSecret, username, password, loginUrl,
                                                    securityToken, connectTimeout, oAuthInfo, proxyUrl);
    this.datetimeAfter = datetimeAfter;
    this.datetimeBefore = datetimeBefore;
    this.duration = duration;
    this.offset = offset;
    this.operation = operation;
  }


  public Map<ChronoUnit, Integer> getDuration() {
    return extractRangeValue(SalesforceSourceConstants.PROPERTY_DURATION, duration);
  }

  public Map<ChronoUnit, Integer> getOffset() {
    return extractRangeValue(SalesforceSourceConstants.PROPERTY_OFFSET, offset);
  }

  @Nullable
  public SalesforceConnectorConfig getConnection() {
    return connection;
  }

  @Nullable
  public String getDatetimeAfter() {
    return datetimeAfter;
  }

  @Nullable
  public String getDatetimeBefore() {
    return datetimeBefore;
  }

  public String getReferenceNameOrNormalizedFQN(String orgId, String sObject) {
    return Strings.isNullOrEmpty(referenceName)
      ? ReferenceNames.normalizeFqn(getFQN(orgId, sObject))
      : referenceName;
  }

  /**
   * Get fully-qualified name (FQN) for a Salesforce object (FQN format: salesforce://prod/orgId.mySobject).
   *
   * @return String fqn
   */
  public String getFQN(String orgId, String sObject) {
    String firstFQNPart = connection.getLoginUrl().equals(DEFAULT_LOGIN_URL) ? "prod" : "sandbox";
    return String.format("salesforce://%s/%s.%s", firstFQNPart, orgId, sObject);
  }

  public String getOrgId(OAuthInfo oAuthInfo) throws ConnectionException {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(oAuthInfo,
                                                                        this.getConnection().getConnectTimeout(),
                                                                        this.connection.getProxyUrl());
    PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
    return partnerConnection.getUserInfo().getOrganizationId();
  }

  public void validateFilters(FailureCollector collector) {
    try {
      validateIntervalFilterProperty(SalesforceSourceConstants.PROPERTY_DATETIME_AFTER, getDatetimeAfter());
    } catch (InvalidConfigException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
    }
    try {
      validateIntervalFilterProperty(SalesforceSourceConstants.PROPERTY_DATETIME_BEFORE, getDatetimeBefore());
    } catch (InvalidConfigException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
    }
    try {
      validateRangeFilterProperty(SalesforceSourceConstants.PROPERTY_DURATION, getDuration());
    } catch (InvalidConfigException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
    }
    try {
      validateRangeFilterProperty(SalesforceSourceConstants.PROPERTY_OFFSET, getOffset());
    } catch (InvalidConfigException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
    }
    try {
      validateOperationProperty(SalesforceSourceConstants.PROPERTY_OPERATION, getOperation());
    } catch (InvalidConfigException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(e.getProperty());
    }
  }

  /**
   * Generates SOQL based on given sObject name metadata and filter properties.
   * Includes only those sObject fields which are present in the schema.
   * Flattens all compound fields by adding individual fields and excludes compound fields names to handle
   * Bulk API limitation.
   * This allows to avoid pulling data from Salesforce for the fields which are not needed.
   *
   * @param sObjectName      Salesforce object name
   * @param schema           CDAP schema
   * @param logicalStartTime application start time
   * @return SOQL generated based on sObject metadata and given filters
   */
  protected String getSObjectQuery(String sObjectName, Schema schema, long logicalStartTime, OAuthInfo oAuthInfo) {
    try {
      AuthenticatorCredentials credentials = new AuthenticatorCredentials(oAuthInfo,
                                                                          this.getConnection().getConnectTimeout(),
                                                                          this.connection.getProxyUrl());
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName,
                                                                       credentials,
                                                                       SalesforceSchemaUtil.COMPOUND_FIELDS);

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

      SObjectFilterDescriptor filterDescriptor = getSObjectFilterDescriptor(logicalStartTime);
      String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fieldNames, sObjectName, filterDescriptor);
      LOG.debug("Generated SObject query: '{}'", sObjectQuery);
      return sObjectQuery;
    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new IllegalStateException(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s' due to error: %s",
                      sObjectName, message), e);
    }
  }

  private SObjectFilterDescriptor getSObjectFilterDescriptor(long logicalStartTime) {
    SObjectFilterDescriptor filterDescriptor;
    ZonedDateTime start = parseDatetime(datetimeAfter);
    ZonedDateTime end = parseDatetime(datetimeBefore);

    filterDescriptor = (start != null || end != null)
      ? SObjectFilterDescriptor.interval(start, end)
      : SObjectFilterDescriptor.range(logicalStartTime, getDuration(), getOffset());
    return filterDescriptor;
  }

  private void validateIntervalFilterProperty(String propertyName, String datetime) {
    if (containsMacro(propertyName)) {
      return;
    }
    try {
      parseDatetime(datetime);
    } catch (DateTimeParseException e) {
      throw new InvalidConfigException(
        String.format("Invalid SObject '%s' value: '%s'. Value must be in Salesforce Date Formats. For example, "
                        + "2019-01-01T23:01:01Z", propertyName, datetime), propertyName);
    }
  }

  private void validateOperationProperty(String propertyName, String operation) {
    try {
      OperationEnum.valueOf(operation);
    } catch (InvalidConfigException e) {
      throw new InvalidConfigException(
        String.format("Invalid Query Operation: '%s'. Valid operation values are query and queryAll.",
                      operation), propertyName);
    }
  }

  private void validateRangeFilterProperty(String propertyName, Map<ChronoUnit, Integer> rangeValue) {
    if (containsMacro(propertyName) || rangeValue.isEmpty()) {
      return;
    }
    List<Map.Entry<ChronoUnit, Integer>> invalidValues = rangeValue.entrySet().stream()
      .filter(e -> e.getValue() < SalesforceConstants.RANGE_FILTER_MIN_VALUE)
      .collect(Collectors.toList());

    if (!invalidValues.isEmpty()) {
      throw new InvalidConfigException(
        String.format("Invalid SObject '%s' values: '%s'. Values must be '%d' or greater", propertyName,
                      invalidValues, SalesforceConstants.RANGE_FILTER_MIN_VALUE), propertyName);
    }
  }

  private Map<ChronoUnit, Integer> extractRangeValue(String propertyName, String rangeValue) {
    if (StringUtils.isBlank(rangeValue)) {
      return Collections.emptyMap();
    }
    return Stream.of(rangeValue.split(","))
      .map(String::trim)
      .map(s -> s.split(" ", 2))
      .peek(keyValue -> validateUnitKeyValue(propertyName, rangeValue, keyValue))
      .collect(Collectors.toMap(
        keyValue -> parseUnitType(propertyName, keyValue[1]),
        keyValue -> parseUnitValue(propertyName, keyValue[0]),
        (o, n) -> {
          throw new InvalidConfigException(
            String.format("'%s' has duplicate unit types '%s'",
                          propertyName, rangeValue), propertyName);
        }
      ));
  }

  private void validateUnitKeyValue(String propertyName, String rangeValue, String[] keyValue) {
    if (keyValue.length < 2) {
      throw new InvalidConfigException(
        String.format("'%s' has invalid format '%s'. "
                        + "Expected format is <VALUE_1> <TYPE_1>,<VALUE_2> <TYPE_2>... . "
                        + "For example, '1 days, 2 hours, 30 minutes'", propertyName, rangeValue), propertyName);
    }
  }

  private ChronoUnit parseUnitType(String propertyName, String value) {

    try {
      return ChronoUnit.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigException(
        String.format("'%s' has invalid unit type '%s'", propertyName, value), e, propertyName);
    }
  }

  private int parseUnitValue(String propertyName, String value) {
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new InvalidConfigException(
        String.format("'%s' has invalid unit value '%s'", propertyName, value), e, propertyName);
    }
  }

  @Nullable
  private ZonedDateTime parseDatetime(String datetime) throws DateTimeParseException {
    return StringUtils.isBlank(datetime) ? null : ZonedDateTime.parse(datetime, DateTimeFormatter.ISO_DATE_TIME);
  }

  public String getOperation() {
    return operation == null ? DEFAULT_OPERATION : operation;
  }
}
