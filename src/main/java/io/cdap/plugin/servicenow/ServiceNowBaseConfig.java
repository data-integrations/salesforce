/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.servicenow;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.SourceValueType;
import org.apache.http.HttpStatus;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * ServiceNow Base Config. Contains connection properties and methods.
 */
public class ServiceNowBaseConfig extends PluginConfig {

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private final ServiceNowConnectorConfig connection;

  public ServiceNowBaseConfig(String clientId, String clientSecret, String restApiEndpoint,
                              String user, String password) {
    this.connection = new ServiceNowConnectorConfig(clientId, clientSecret, restApiEndpoint, user, password);
  }

  @Nullable
  public ServiceNowConnectorConfig getConnection() {
    return connection;
  }

  /**
   * Validates {@link ServiceNowSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    validateCredentials(collector);
  }

  public void validateCredentials(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }
    if (connection != null) {
      connection.validateCredentialsFields(collector);
      validateServiceNowConnection(collector);
    }
  }

  @VisibleForTesting
  public void validateServiceNowConnection(FailureCollector collector) {
    try {
      ServiceNowTableAPIClientImpl restApi = new ServiceNowTableAPIClientImpl(connection);
      restApi.getAccessToken();
    } catch (Exception e) {
      collector.addFailure("Unable to connect to ServiceNow Instance.",
                           "Ensure properties like Client ID, Client Secret, API Endpoint, User Name, Password " +
                             "are correct.")
        .withConfigProperty(ServiceNowConstants.PROPERTY_CLIENT_ID)
        .withConfigProperty(ServiceNowConstants.PROPERTY_CLIENT_SECRET)
        .withConfigProperty(ServiceNowConstants.PROPERTY_API_ENDPOINT)
        .withConfigProperty(ServiceNowConstants.PROPERTY_USER)
        .withConfigProperty(ServiceNowConstants.PROPERTY_PASSWORD)
        .withStacktrace(e.getStackTrace());
    }
  }

  /**
   * Returns true if ServiceNow can be connected to.
   */
  public boolean shouldConnect() {
    return !containsMacro(ServiceNowConstants.PROPERTY_CLIENT_ID) &&
      !containsMacro(ServiceNowConstants.PROPERTY_CLIENT_SECRET) &&
      !containsMacro(ServiceNowConstants.PROPERTY_API_ENDPOINT) &&
      !containsMacro(ServiceNowConstants.PROPERTY_USER) &&
      !containsMacro(ServiceNowConstants.PROPERTY_PASSWORD);
  }

  public boolean shouldGetSchema() {
    return !containsMacro(ServiceNowConstants.PROPERTY_QUERY_MODE)
      && !containsMacro(ServiceNowConstants.PROPERTY_APPLICATION_NAME)
      && !containsMacro(ServiceNowConstants.PROPERTY_TABLE_NAME_FIELD)
      && !containsMacro(ServiceNowConstants.PROPERTY_TABLE_NAME)
      && !containsMacro(ServiceNowConstants.PROPERTY_TABLE_NAMES)
      && shouldConnect()
      && !containsMacro(ServiceNowConstants.PROPERTY_VALUE_TYPE);
  }

  public void validateTable(String tableName, SourceValueType valueType, FailureCollector collector,
                            String tableField) {
    // Call API to fetch first record from the table
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      connection.getRestApiEndpoint(), tableName, false)
      .setExcludeReferenceLink(true)
      .setDisplayValue(valueType)
      .setLimit(1);

    RestAPIResponse apiResponse = null;
    ServiceNowTableAPIClientImpl serviceNowTableAPIClient = new ServiceNowTableAPIClientImpl(connection);
    try {
      String accessToken = serviceNowTableAPIClient.getAccessToken();
      requestBuilder.setAuthHeader(accessToken);

      // Get the response JSON and fetch the header X-Total-Count. Set the value to recordCount
      requestBuilder.setResponseHeaders(ServiceNowConstants.HEADER_NAME_TOTAL_COUNT);

      apiResponse = serviceNowTableAPIClient.executeGetWithRetries(requestBuilder.build());
      if (serviceNowTableAPIClient.parseResponseToResultListOfMap(apiResponse.getResponseBody()).isEmpty()) {
        // Removed config property as in case of MultiSource, only first table error was populating.
        collector.addFailure("Table: " + tableName + " is empty.", "");
      }
    } catch (Exception e) {
      collector.addFailure(String.format("ServiceNow API returned an unexpected result or the specified table may " +
                              "not exist. Cause: %s", e.getMessage()),
                           "Ensure specified table exists in the datasource. ")
        .withConfigProperty(ServiceNowConstants.PROPERTY_TABLE_NAME);
    }
  }

}
