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

package co.cask.hydrator.salesforce.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.salesforce.SalesforceConnectionUtil;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import com.sforce.ws.ConnectionException;

/**
 * Base configuration for Salesforce Streaming and Batch plugins
 */
public class BaseSalesforceConfig extends ReferencePluginConfig {
  private static final String PROPERTY_CLIENTID = "clientId";
  private static final String PROPERTY_CLIENT_SECRET = "clientSecret";
  private static final String PROPERTY_USERNAME = "username";
  private static final String PROPERTY_PASSWORD = "password";
  private static final String PROPERTY_LOGINURL = "loginUrl";

  public static final String ERROR_HANDLING_SKIP = "Skip on error";
  public static final String ERROR_HANDLING_SEND = "Send to error";
  public static final String ERROR_HANDLING_STOP = "Stop on error";

  @Description("Salesforce connected app's client ID")
  @Macro
  private String clientId;

  @Description("Salesforce connected app's client secret key")
  @Macro
  private String clientSecret;

  @Description("Salesforce username")
  @Macro
  private String username;

  @Description("Salesforce password")
  @Macro
  private String password;

  @Description("Endpoint to authenticate to")
  @Macro
  private final String loginUrl;

  @Description("Strategy used to handle erroneous records. Acceptable values are Skip on error,\n" +
    "Send to error, Stop on error.\n" +
    "\n" +
    "Skip on error - ignores erroneous record.\n" +
    "Send to error - emits an error to error handler. " +
    "Errors are records with a field 'body', containing erroneous row.\n" +
    "Stop on error - fails pipeline due to erroneous record.")
  private final String errorHandling;

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

  public String getErrorHandling() {
    return errorHandling;
  }

  public void validate() {
    if (containsMacro(PROPERTY_CLIENTID) || containsMacro(PROPERTY_CLIENT_SECRET) ||
        containsMacro(PROPERTY_USERNAME) || containsMacro(PROPERTY_PASSWORD) ||
        containsMacro(PROPERTY_LOGINURL)) {
        return;
    }

    try {
      SalesforceConnectionUtil.getPartnerConnection(this.getAuthenticatorCredentials());
    } catch (ConnectionException | IllegalArgumentException e) {
      String errorMessage = "Cannot connect to Salesforce API with credentials specified";
      throw new IllegalArgumentException(errorMessage, e);
    }
  }

  public AuthenticatorCredentials getAuthenticatorCredentials() {
    return SalesforceConnectionUtil.getAuthenticatorCredentials(this.username, this.password,
                                    this.clientId, this.clientSecret,
                                    this.loginUrl);
  }
}
