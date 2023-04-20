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

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;

import javax.annotation.Nullable;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class BaseSalesforceConfig extends ReferencePluginConfig {

  @Nullable
  @Name(SalesforceConstants.PROPERTY_PROXY_URL)
  @Description("Proxy URL. Must contain a protocol, address and port.")
  @Macro
  protected final String proxyUrl;

  @Name(SalesforceConstants.PROPERTY_OAUTH_INFO)
  @Description("OAuth information for connecting to Salesforce. " +
    "It is expected to be an json string containing two properties, \"accessToken\" and \"instanceURL\", " +
    "which carry the OAuth access token and the URL to connect to respectively. " +
    "Use the ${oauth(provider, credentialId)} macro function for acquiring OAuth information dynamically. ")
  @Macro
  @Nullable
  private OAuthInfo oAuthInfo;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_KEY)
  @Description("Salesforce connected app's consumer key")
  @Macro
  @Nullable
  private String consumerKey;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
  @Description("Salesforce connected app's client secret key")
  @Macro
  @Nullable
  private String consumerSecret;

  @Name(SalesforceConstants.PROPERTY_USERNAME)
  @Description("Salesforce username")
  @Macro
  @Nullable
  private String username;

  @Name(SalesforceConstants.PROPERTY_PASSWORD)
  @Description("Salesforce password")
  @Macro
  @Nullable
  private String password;

  @Name(SalesforceConstants.PROPERTY_SECURITY_TOKEN)
  @Description("Salesforce security token")
  @Macro
  @Nullable
  private String securityToken;

  @Name(SalesforceConstants.PROPERTY_LOGIN_URL)
  @Description("Endpoint to authenticate to")
  @Macro
  @Nullable
  private String loginUrl;

  @Name(SalesforceConstants.PROPERTY_CONNECT_TIMEOUT)
  @Description("Maximum time in milliseconds to wait for connection initialization before time out.")
  @Macro
  @Nullable
  private final Integer connectTimeout;

  public BaseSalesforceConfig(String referenceName,
                              @Nullable String consumerKey,
                              @Nullable String consumerSecret,
                              @Nullable String username,
                              @Nullable String password,
                              @Nullable String loginUrl,
                              @Nullable String securityToken,
                              @Nullable Integer connectTimeout,
                              @Nullable OAuthInfo oAuthInfo,
                              @Nullable String proxyUrl) {
    super(referenceName);
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
    this.securityToken = securityToken;
    this.connectTimeout = connectTimeout;
    this.oAuthInfo = oAuthInfo;
    this.proxyUrl = proxyUrl;
  }

  @Nullable
  public OAuthInfo getOAuthInfo() {
    return oAuthInfo;
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

  @Nullable
  public Integer getConnectTimeout() {
    if (connectTimeout == null) {
      return SalesforceConstants.DEFAULT_CONNECTION_TIMEOUT_MS;
    }
    return connectTimeout;
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

  public AuthenticatorCredentials getAuthenticatorCredentials() {
    OAuthInfo oAuthInfo = getOAuthInfo();
    if (oAuthInfo != null) {
      return new AuthenticatorCredentials(oAuthInfo, getConnectTimeout(),
                                          getProxyUrl());
    }
    return new AuthenticatorCredentials(getUsername(), getPassword(), getConsumerKey(),
                                        getConsumerSecret(), getLoginUrl(), getConnectTimeout(),
                                        getProxyUrl());
  }

  /**
   * Checks if current config does not contain macro for properties which are used
   * to establish connection to Salesforce.
   *
   * @return true if none of the connection properties contains macro, false otherwise
   */
  public boolean canAttemptToEstablishConnection() {
    // If OAuth token is configured, use it to establish connection
    if (getOAuthInfo() != null) {
      return true;
    }

    // At configurePipeline time, macro is not resolved, hence the OAuth field will be null.
    if (containsMacro(SalesforceConstants.PROPERTY_OAUTH_INFO)) {
      return false;
    }

    return !(containsMacro(SalesforceConstants.PROPERTY_CONSUMER_KEY)
      || containsMacro(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
      || containsMacro(SalesforceConstants.PROPERTY_USERNAME)
      || containsMacro(SalesforceConstants.PROPERTY_PASSWORD)
      || containsMacro(SalesforceConstants.PROPERTY_LOGIN_URL)
      || containsMacro(SalesforceConstants.PROPERTY_SECURITY_TOKEN)
      || containsMacro(SalesforceConstants.PROPERTY_CONNECT_TIMEOUT));
  }

  private void validateConnection(@Nullable OAuthInfo oAuthInfo) {
    if (oAuthInfo == null) {
      return;
    }

    try {
      SalesforceConnectionUtil.getPartnerConnection(new AuthenticatorCredentials(oAuthInfo, this.getConnectTimeout(),
                                                                                 getProxyUrl()));
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

}
