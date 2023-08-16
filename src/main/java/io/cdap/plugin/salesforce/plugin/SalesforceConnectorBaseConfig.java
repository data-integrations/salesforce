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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;

import javax.annotation.Nullable;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class SalesforceConnectorBaseConfig extends PluginConfig {

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
  @Description("Endpoint to authenticate to")
  @Macro
  @Nullable
  private final String loginUrl;

  public SalesforceConnectorBaseConfig(@Nullable String consumerKey,
                                       @Nullable String consumerSecret,
                                       @Nullable String username,
                                       @Nullable String password,
                                       @Nullable String loginUrl,
                                       @Nullable String securityToken,
                                       @Nullable Integer connectTimeout,
                                       @Nullable String proxyUrl) {
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
    this.securityToken = securityToken;
    this.connectTimeout = connectTimeout;
    this.proxyUrl = proxyUrl;
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
