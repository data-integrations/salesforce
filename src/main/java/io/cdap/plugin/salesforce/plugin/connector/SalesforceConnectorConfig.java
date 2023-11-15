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
package io.cdap.plugin.salesforce.plugin.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorBaseConfig;

import javax.annotation.Nullable;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class SalesforceConnectorConfig extends SalesforceConnectorBaseConfig {

  @Name(SalesforceConstants.PROPERTY_OAUTH_INFO)
  @Description("OAuth information for connecting to Salesforce. " +
    "It is expected to be an json string containing two properties, \"accessToken\" and \"instanceURL\", " +
    "which carry the OAuth access token and the URL to connect to respectively. " +
    "Use the ${oauth(provider, credentialId)} macro function for acquiring OAuth information dynamically. ")
  @Macro
  @Nullable
  private OAuthInfo oAuthInfo;

  public SalesforceConnectorConfig(@Nullable String consumerKey,
                                   @Nullable String consumerSecret,
                                   @Nullable String username,
                                   @Nullable String password,
                                   @Nullable String loginUrl,
                                   @Nullable String securityToken,
                                   @Nullable Integer connectTimeout,
                                   @Nullable Integer readTimeout,
                                   @Nullable OAuthInfo oAuthInfo,
                                   @Nullable String proxyUrl) {
    super(consumerKey, consumerSecret, username, password, loginUrl, securityToken, connectTimeout, readTimeout,
          proxyUrl);
    this.oAuthInfo = oAuthInfo;
  }

  @Nullable
  public OAuthInfo getOAuthInfo() {
    return oAuthInfo;
  }
}
