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
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectsDescribeResult;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Holds configuration properties for Salesforce Batch Multi Source plugin.
 */
public class SalesforceMultiSourceConfig extends SalesforceBaseSourceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceMultiSourceConfig.class);

  public static final String SOBJECT_NAME_FIELD_DEFAULT = "tablename";

  @Name(SalesforceSourceConstants.PROPERTY_WHITE_LIST)
  @Macro
  @Nullable
  @Description("List of SObjects to fetch from Salesforce. By default all SObjects will be white listed")
  private String whiteList;

  @Name(SalesforceSourceConstants.PROPERTY_BLACK_LIST)
  @Macro
  @Nullable
  @Description("List of SObjects NOT to fetch from Salesforce. By default NONE of SObjects will be black listed")
  private String blackList;

  @Name(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME_FIELD)
  @Nullable
  @Description("The name of the field that holds SObject name. "
    + "Must not be the name of any sObject column that will be read. Defaults to 'tablename'.")
  private String sObjectNameField;

  public SalesforceMultiSourceConfig(String referenceName,
                              String consumerKey,
                              String consumerSecret,
                              String username,
                              String password,
                              String loginUrl,
                              @Nullable String datetimeAfter,
                              @Nullable String datetimeBefore,
                              @Nullable String duration,
                              @Nullable String offset,
                              @Nullable String whiteList,
                              @Nullable String blackList,
                              @Nullable String sObjectNameField,
                              @Nullable String securityToken,
                              @Nullable String operation) {
    super(referenceName, consumerKey, consumerSecret, username, password, loginUrl,
          datetimeAfter, datetimeBefore, duration, offset, securityToken);
    this.whiteList = whiteList;
    this.blackList = blackList;
    this.sObjectNameField = sObjectNameField;
  }

  public Set<String> getWhiteList() {
    return getList(whiteList);
  }

  public Set<String> getBlackList() {
    return getList(blackList);
  }

  public String getSObjectNameField() {
    return Strings.isNullOrEmpty(sObjectNameField) ? SOBJECT_NAME_FIELD_DEFAULT : sObjectNameField;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateFilters(collector);
  }

  /**
   * Generates CDAP schema for each SObject. Collects generated schemas into map
   * where key is SObject name, value is corresponding CDAP schema.
   *
   * @return map of SObjects schemas
   */
  public Map<String, Schema> getSObjectsSchemas(List<String> queries) throws ConnectionException {
    List<SObjectDescriptor> sObjectDescriptors = queries.parallelStream()
      .map(SObjectDescriptor::fromQuery)
      .collect(Collectors.toList());

    PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(getAuthenticatorCredentials());

    Set<String> sObjectsToDescribe = sObjectDescriptors.stream()
      .map(SObjectDescriptor::getName)
      .collect(Collectors.toSet());

    // generate one describe result for all SObjects in one request to Salesforce
    SObjectsDescribeResult describeResult = SObjectsDescribeResult.of(partnerConnection, sObjectsToDescribe);

    return sObjectDescriptors.stream()
      .collect(Collectors.toMap(
        SObjectDescriptor::getName,
        sObjectDescriptor -> SalesforceSchemaUtil.getSchemaWithFields(sObjectDescriptor, describeResult),
        (o, n) -> n
      ));
  }

  /**
   * Generates list of SObject queries based on given list of SObjects and incremental filters if any.
   *
   * @param logicalStartTime application start time
   * @return list of SObject queries
   */
  public List<String> getQueries(long logicalStartTime) {
    List<String> queries = getSObjects().parallelStream()
      .map(sObject -> getSObjectQuery(sObject, null, logicalStartTime))
      .collect(Collectors.toList());

    if (queries.isEmpty()) {
      throw new IllegalArgumentException("No SObject queries are generated");
    }

    LOG.debug("Generated '{}' SObject queries", queries.size());
    return queries;
  }

  /**
   * Retrieves all queryable SObjects in Salesforce and applies white and black list filters.
   *
   * @return list of SObjects
   */
  private List<String> getSObjects() {
    DescribeGlobalResult describeGlobalResult;
    try {
      PartnerConnection partnerConnection =
        SalesforceConnectionUtil.getPartnerConnection(getAuthenticatorCredentials());
      describeGlobalResult = partnerConnection.describeGlobal();
    } catch (ConnectionException e) {
      throw new IllegalArgumentException("Unable to connect to Salesforce", e);
    }

    Set<String> whileList = getWhiteList();
    Set<String> blackList = getBlackList();

    List<String> sObjects = Stream.of(describeGlobalResult.getSobjects())
      .filter(DescribeGlobalSObjectResult::getQueryable)
      .map(DescribeGlobalSObjectResult::getName)
      .filter(name -> whileList.isEmpty() || whileList.contains(name))
      .filter(name -> !blackList.contains(name))
      .collect(Collectors.toList());

    if (sObjects.isEmpty()) {
      throw new IllegalArgumentException("No qualified SObjects are found");
    }

    LOG.debug("SObjects to be replicated: '{}'", sObjects);
    return sObjects;
  }

  private Set<String> getList(String value) {
    return Strings.isNullOrEmpty(value)
      ? Collections.emptySet()
      : Stream.of(value.split(","))
      .map(String::trim)
      .filter(name -> !name.isEmpty())
      .collect(Collectors.toSet());
  }

}
