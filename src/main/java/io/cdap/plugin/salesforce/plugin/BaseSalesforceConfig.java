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
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class BaseSalesforceConfig extends ReferencePluginConfig {

  @Name(SalesforceConstants.PROPERTY_CLIENT_ID)
  @Description("Salesforce connected app's client ID")
  @Macro
  private String clientId;

  @Name(SalesforceConstants.PROPERTY_CLIENT_SECRET)
  @Description("Salesforce connected app's client secret key")
  @Macro
  private String clientSecret;

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

  @Name(SalesforceConstants.PROPERTY_ERROR_HANDLING)
  @Description("Strategy used to handle erroneous records. Acceptable values are Skip on error, " +
    "Send to error, Stop on error.\n" +
    "Skip on error - ignores erroneous record.\n" +
    "Send to error - emits an error to error handler. " +
    "Errors are records with a field 'body', containing erroneous row.\n" +
    "Stop on error - fails pipeline due to erroneous record.")
  @Macro
  private String errorHandling;

  public BaseSalesforceConfig(String referenceName, String clientId, String clientSecret,
                              String username, String password, String loginUrl, String errorHandling) {
    super(referenceName);
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
    this.errorHandling = errorHandling;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
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

  public ErrorHandling getErrorHandling() {
    return ErrorHandling.fromValue(errorHandling)
      .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported error handling value: " + errorHandling,
        SalesforceConstants.PROPERTY_ERROR_HANDLING));
  }

  public void validate() {
    validateConnection();
    validateErrorHandling();
  }

  public AuthenticatorCredentials getAuthenticatorCredentials() {
    return SalesforceConnectionUtil.getAuthenticatorCredentials(username, password, clientId, clientSecret, loginUrl);
  }

  /**
   * Checks if current config does not contain macro for properties which are used
   * to establish connection to Salesforce.
   *
   * @return true if none of the connection properties contains macro, false otherwise
   */
  public boolean canAttemptToEstablishConnection() {
    return !(containsMacro(SalesforceConstants.PROPERTY_CLIENT_ID)
      || containsMacro(SalesforceConstants.PROPERTY_CLIENT_SECRET)
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

  private void validateErrorHandling() {
    if (containsMacro(SalesforceConstants.PROPERTY_ERROR_HANDLING)) {
      return;
    }

    getErrorHandling();
  }

}
