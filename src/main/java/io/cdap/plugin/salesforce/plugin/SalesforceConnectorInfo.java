/*
 * Copyright © 2023 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.connector.SalesforceConnectorConfig;

import javax.annotation.Nullable;

/**
 * Common logic and configuration for Connector, Source, and Sink plugins. This class exists because the
 * {@link SalesforceConnectorConfig} cannot be used directly in Source and Sink configs because plugin configs
 * cannot contain an object (OAuthInfo) within an object (SalesforceConnectorConfig).
 */
public class SalesforceConnectorInfo {
  private final OAuthInfo oAuthInfo;
  private final SalesforceConnectorBaseConfig config;

  private final boolean isOAuthInfoMacro;

  public SalesforceConnectorInfo(@Nullable OAuthInfo oAuthInfo, SalesforceConnectorBaseConfig config,
                                 boolean isOAuthInfoMacro) {
    this.oAuthInfo = oAuthInfo;
    this.config = config;
    this.isOAuthInfoMacro = isOAuthInfoMacro;
  }

  @Nullable
  public OAuthInfo getOAuthInfo() {
    return oAuthInfo;
  }

  @Nullable
  public String getConsumerKey() {
    return config.getConsumerKey();
  }

  @Nullable
  public String getConsumerSecret() {
    return config.getConsumerSecret();
  }

  @Nullable
  public String getUsername() {
    return config.getUsername();
  }

  @Nullable
  public String getPassword() {
    return config.getPassword();
  }

  @Nullable
  public String getLoginUrl() {
    return config.getLoginUrl();
  }

  @Nullable
  public Integer getConnectTimeout() {
    return config.getConnectTimeout();
  }

  @Nullable
  public Integer getReadTimeout() {
    return config.getReadTimeoutInMillis();
  }

  @Nullable
  public String getProxyUrl() {
    return config.getProxyUrl();
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
      return new AuthenticatorCredentials(oAuthInfo, config.getConnectTimeout(), config.getReadTimeoutInMillis(),
                                          config.getProxyUrl());
    }
    return new AuthenticatorCredentials(config.getUsername(), config.getPassword(), config.getConsumerKey(),
                                        config.getConsumerSecret(), config.getLoginUrl(), config.getConnectTimeout(),
                                        config.getReadTimeoutInMillis(), config.getProxyUrl());
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
    if (isOAuthInfoMacro) {
      return false;
    }

    return !(config.containsMacro(SalesforceConstants.PROPERTY_CONSUMER_KEY)
      || config.containsMacro(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
      || config.containsMacro(SalesforceConstants.PROPERTY_USERNAME)
      || config.containsMacro(SalesforceConstants.PROPERTY_PASSWORD)
      || config.containsMacro(SalesforceConstants.PROPERTY_LOGIN_URL)
      || config.containsMacro(SalesforceConstants.PROPERTY_SECURITY_TOKEN)
      || config.containsMacro(SalesforceConstants.PROPERTY_CONNECT_TIMEOUT)
      || config.containsMacro(SalesforceConstants.PROPERTY_READ_TIMEOUT));
  }

  private void validateConnection(@Nullable OAuthInfo oAuthInfo) {
    if (oAuthInfo == null) {
      return;
    }

    try {
      SalesforceConnectionUtil.getPartnerConnection(new AuthenticatorCredentials(oAuthInfo, config.getConnectTimeout(),
                                                                                 config.getReadTimeoutInMillis(),
                                                                                 config.getProxyUrl()));
    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
        String.format("Failed to establish and validate connection to salesforce: %s", message), e);
    }
  }
}
