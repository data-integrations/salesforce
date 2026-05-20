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
package io.cdap.plugin.salesforce.plugin;

import com.google.common.base.Strings;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials.GrantType;

import javax.annotation.Nullable;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class SalesforceConnectorBaseConfig extends PluginConfig {

  @Name(SalesforceConstants.PROPERTY_AUTHENTICATION_GRANT_TYPE)
  @Description("Salesforce authentication grant type: 'password' or 'client_credentials'")
  @Nullable
  @Macro
  protected String authenticationGrantType;

  @Nullable
  @Name(SalesforceConstants.PROPERTY_PROXY_URL)
  @Description("Proxy URL. Must contain a protocol, address and port.")
  @Macro
  protected final String proxyUrl;

  @Name(SalesforceConstants.PROPERTY_CONNECT_TIMEOUT)
  @Description("Maximum time in milliseconds to wait for connection initialization before time out.")
  @Macro
  @Nullable
  private final Integer connectTimeout;

  @Name(SalesforceConstants.PROPERTY_READ_TIMEOUT)
  @Description("Maximum time in seconds to wait for reading data from the server before it times out.")
  @Macro
  @Nullable
  private final Integer readTimeout;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_KEY)
  @Description("Salesforce connected app's consumer key")
  @Macro
  @Nullable
  private final String consumerKey;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
  @Description("Salesforce connected app's client secret key")
  @Macro
  @Nullable
  private final String consumerSecret;

  @Name(SalesforceConstants.PROPERTY_USERNAME)
  @Description("Salesforce username")
  @Macro
  @Nullable
  private final String username;

  @Name(SalesforceConstants.PROPERTY_PASSWORD)
  @Description("Salesforce password")
  @Macro
  @Nullable
  private final String password;

  @Name(SalesforceConstants.PROPERTY_SECURITY_TOKEN)
  @Description("Salesforce security token")
  @Macro
  @Nullable
  private final String securityToken;

  @Name(SalesforceConstants.PROPERTY_LOGIN_URL)
  @Description("Salesforce OAuth2 login URL. For the 'password' grant type, the default generic URL\n" +
          "`https://login.salesforce.com/services/oauth2/token` can be used. " +
          "For the 'client_credentials' grant type,\n" +
          "you must provide your Salesforce instance-specific URL, for example\n" +
          "`https://<your-instance>.my.salesforce.com/services/oauth2/token`.")
  @Macro
  @Nullable
  private final String loginUrl;

  @Name(SalesforceConstants.PROPERTY_INITIAL_RETRY_DURATION)
  @Description("Time taken for the first retry. Default is 5 seconds.")
  @Nullable
  private final Long initialRetryDuration;

  @Name(SalesforceConstants.PROPERTY_MAX_RETRY_DURATION)
  @Description("Maximum time in seconds retries can take. Default is 80 seconds.")
  @Nullable
  private final Long maxRetryDuration;

  @Name(SalesforceConstants.PROPERTY_MAX_RETRY_COUNT)
  @Description("Maximum number of retries allowed. Default is 5.")
  @Nullable
  private final Integer maxRetryCount;

  @Name(SalesforceConstants.PROPERTY_RETRY_REQUIRED)
  @Description("Retry is required or not for some of the internal call failures")
  @Nullable
  private final Boolean retryOnBackendError;

  public SalesforceConnectorBaseConfig(@Nullable String consumerKey,
                                       @Nullable String consumerSecret,
                                       @Nullable String username,
                                       @Nullable String password,
                                       @Nullable String loginUrl,
                                       @Nullable String securityToken,
                                       @Nullable Integer connectTimeout,
                                       @Nullable Integer readTimeout,
                                       @Nullable String proxyUrl,
                                       @Nullable Long initialRetryDuration,
                                       @Nullable Long maxRetryDuration,
                                       @Nullable Integer maxRetryCount,
                                       @Nullable Boolean retryOnBackendError,
                                       @Nullable String authenticationGrantType) {
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
    this.securityToken = securityToken;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.proxyUrl = proxyUrl;
    this.initialRetryDuration = initialRetryDuration;
    this.maxRetryDuration = maxRetryDuration;
    this.retryOnBackendError = retryOnBackendError;
    this.maxRetryCount = maxRetryCount;
    this.authenticationGrantType = authenticationGrantType;
  }

  public GrantType getAuthenticationGrantType() {
    if (!Strings.isNullOrEmpty(authenticationGrantType) &&
            authenticationGrantType.equals(GrantType.CLIENT_CREDENTIALS.getType())) {
      return GrantType.CLIENT_CREDENTIALS;
    }
    // Default auth, handles null case when upgrading pipeline
    return SalesforceConstants.DEFAULT_GRANT_TYPE;
  }

  @Nullable
  public String getConsumerKey() {
    return consumerKey;
  }

  @Nullable
  public String getConsumerSecret() {
    return consumerSecret;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  @Nullable
  public String getPassword() {
    return constructPasswordWithToken(password, securityToken);
  }

  @Nullable
  public String getLoginUrl() {
    return loginUrl;
  }

  public Long getInitialRetryDuration() {
    return initialRetryDuration == null ? SalesforceConstants.DEFAULT_INITIAL_RETRY_DURATION_SECONDS :
      initialRetryDuration;
  }

  public Long getMaxRetryDuration() {
    return maxRetryDuration == null ? SalesforceConstants.DEFAULT_MAX_RETRY_DURATION_SECONDS : maxRetryDuration;
  }

  public Integer getMaxRetryCount() {
    return maxRetryCount == null ? SalesforceConstants.DEFAULT_MAX_RETRY_COUNT : maxRetryCount;
  }

  public Boolean isRetryOnBackendError() {
    return retryOnBackendError == null || retryOnBackendError;
  }

  @Nullable
  public Integer getConnectTimeout() {
    if (connectTimeout == null) {
      return SalesforceConstants.DEFAULT_CONNECTION_TIMEOUT_MS;
    }
    return connectTimeout;
  }

  @Nullable
  public Integer getReadTimeoutInMillis() {
    if (readTimeout == null) {
      return SalesforceConstants.DEFAULT_READ_TIMEOUT_SEC * 1000;
    }
    return readTimeout * 1000;
  }

  public void validate(FailureCollector collector, @Nullable OAuthInfo oAuthInfo) {
    try {
      validateConnection(oAuthInfo);
    } catch (Exception e) {
      collector.addFailure("Error encountered while establishing connection: " + e.getMessage(),
                           "Please verify authentication properties are provided correctly")
        .withStacktrace(e.getStackTrace());
    }
    collector.getOrThrowException();
  }

  private void validateConnection(@Nullable OAuthInfo oAuthInfo) {
    if (oAuthInfo == null) {
      return;
    }
    AuthenticatorCredentials credentials = AuthenticatorCredentials.fromParameters(
      oAuthInfo, this.getConnectTimeout(), this.getReadTimeoutInMillis(), getProxyUrl(),
      getInitialRetryDuration(), getMaxRetryDuration(), getMaxRetryCount(), isRetryOnBackendError());
    try {
      SalesforceConnectionUtil.getPartnerConnection(credentials);
    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
        String.format("Failed to establish and validate connection to salesforce: %s", message), e);
    }
  }

  private String constructPasswordWithToken(String password, @Nullable String securityToken) {
    if (securityToken != null && !securityToken.isEmpty() && !password.endsWith(securityToken)) {
      return password + securityToken;
    } else {
      return password;
    }
  }

  @Nullable
  public String getProxyUrl() {
    return proxyUrl;
  }

  /**
   * Validates that required authentication fields are present based on the selected OAuth grant type.
   * For PASSWORD grant type: consumerKey, consumerSecret, username, password, and loginUrl are required.
   * For CLIENT_CREDENTIALS grant type: consumerKey, consumerSecret, and loginUrl are required.
   *
   * @param collector the failure collector to report validation errors
   */
  public void validateAuthenticationFields(FailureCollector collector) {
    if (containsMacro(SalesforceConstants.PROPERTY_AUTHENTICATION_GRANT_TYPE)) {
      return;
    }

    // Fields required for all grant types
    if (!containsMacro(SalesforceConstants.PROPERTY_CONSUMER_KEY) && Strings.isNullOrEmpty(consumerKey)) {
      collector.addFailure("Consumer Key is required for authentication.",
                           "Please provide the Consumer Key from your Salesforce connected app.")
        .withConfigProperty(SalesforceConstants.PROPERTY_CONSUMER_KEY);
    }
    if (!containsMacro(SalesforceConstants.PROPERTY_CONSUMER_SECRET) && Strings.isNullOrEmpty(consumerSecret)) {
      collector.addFailure("Consumer Secret is required for authentication.",
                           "Please provide the Consumer Secret from your Salesforce connected app.")
        .withConfigProperty(SalesforceConstants.PROPERTY_CONSUMER_SECRET);
    }
    if (!containsMacro(SalesforceConstants.PROPERTY_LOGIN_URL) && Strings.isNullOrEmpty(loginUrl)) {
      collector.addFailure("Login URL is required for authentication.",
                           "Please provide the Salesforce login URL.")
        .withConfigProperty(SalesforceConstants.PROPERTY_LOGIN_URL);
    }

    GrantType grantType = getAuthenticationGrantType();
    // Fields required only for PASSWORD grant type
    if (grantType == GrantType.PASSWORD) {
      if (!containsMacro(SalesforceConstants.PROPERTY_USERNAME) && Strings.isNullOrEmpty(username)) {
        collector.addFailure("Username is required for password grant type authentication.",
                             "Please provide the Salesforce username.")
          .withConfigProperty(SalesforceConstants.PROPERTY_USERNAME);
      }
      if (!containsMacro(SalesforceConstants.PROPERTY_PASSWORD) && Strings.isNullOrEmpty(password)) {
        collector.addFailure("Password is required for password grant type authentication.",
                             "Please provide the Salesforce password.")
          .withConfigProperty(SalesforceConstants.PROPERTY_PASSWORD);
      }
      if (!containsMacro(SalesforceConstants.PROPERTY_SECURITY_TOKEN) && Strings.isNullOrEmpty(securityToken)) {
        collector.addFailure("Security Token is required for password grant type authentication.",
                             "Please provide the Salesforce security token.")
          .withConfigProperty(SalesforceConstants.PROPERTY_SECURITY_TOKEN);
      }
    }
  }

}
