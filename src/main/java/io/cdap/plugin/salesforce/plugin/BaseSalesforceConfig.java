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
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class BaseSalesforceConfig extends ReferencePluginConfig {

  @Name(SalesforceConstants.PROPERTY_CONSUMER_KEY)
  @Description("Salesforce connected app's consumer key")
  @Macro
  private String consumerKey;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
  @Description("Salesforce connected app's client secret key")
  @Macro
  private String consumerSecret;

  @Name(SalesforceConstants.PROPERTY_USERNAME)
  @Description("Salesforce username")
  @Macro
  private String username;

  @Name(SalesforceConstants.PROPERTY_PASSWORD)
  @Description("Salesforce password")
  @Macro
  private String password;

  @Name(SalesforceConstants.PROPERTY_LOGIN_URL)
  @Description("Endpoint to authenticate to")
  @Macro
  private String loginUrl;

  public BaseSalesforceConfig(String referenceName, String consumerKey, String consumerSecret,
                              String username, String password, String loginUrl) {
    super(referenceName);
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
  }

  public String getConsumerKey() {
    return consumerKey;
  }

  public String getConsumerSecret() {
    return consumerSecret;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getLoginUrl() {
    return loginUrl;
  }

  public void validate() {
    validateConnection();
  }

  public AuthenticatorCredentials getAuthenticatorCredentials() {
    return SalesforceConnectionUtil.getAuthenticatorCredentials(username, password,
                                                                consumerKey, consumerSecret, loginUrl);
  }

  /**
   * Checks if current config does not contain macro for properties which are used
   * to establish connection to Salesforce.
   *
   * @return true if none of the connection properties contains macro, false otherwise
   */
  public boolean canAttemptToEstablishConnection() {
    return !(containsMacro(SalesforceConstants.PROPERTY_CONSUMER_KEY)
      || containsMacro(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
      || containsMacro(SalesforceConstants.PROPERTY_USERNAME)
      || containsMacro(SalesforceConstants.PROPERTY_PASSWORD)
      || containsMacro(SalesforceConstants.PROPERTY_LOGIN_URL));
  }

  private void validateConnection() {
    if (!canAttemptToEstablishConnection()) {
      return;
    }

    try {
      SalesforceConnectionUtil.getPartnerConnection(this.getAuthenticatorCredentials());
    } catch (ConnectionException | IllegalArgumentException e) {
      String errorMessage = "Cannot connect to Salesforce API with credentials specified";
      throw new IllegalArgumentException(errorMessage, e);
    }
  }
}
