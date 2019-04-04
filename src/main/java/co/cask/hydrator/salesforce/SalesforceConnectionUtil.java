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
package co.cask.hydrator.salesforce;

import co.cask.hydrator.salesforce.authenticator.Authenticator;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility class which provides methods to establish connection with Salesforce.
 */
public class SalesforceConnectionUtil {

  /**
   * Based on given Salesforce credentials, attempt to establish {@link PartnerConnection}.
   * This is mainly used to obtain sObject describe results.
   *
   * @param credentials Salesforce credentials
   * @return partner connection instance
   * @throws ConnectionException in case error when establishing connection
   */
  public static PartnerConnection getPartnerConnection(AuthenticatorCredentials credentials)
    throws ConnectionException {
    ConnectorConfig connectorConfig = Authenticator.createConnectorConfig(credentials);
    return new PartnerConnection(connectorConfig);
  }

  /**
   * Creates {@link AuthenticatorCredentials} instance based on given parameters.
   *
   * @param username Salesforce username
   * @param password Salesforce password
   * @param clientId Salesforce client id
   * @param clientSecret Salesforce client secret
   * @param loginUrl Salesforce authentication url
   * @return authenticator credentials
   */
  public static AuthenticatorCredentials getAuthenticatorCredentials(String username, String password, String clientId,
                                                                     String clientSecret, String loginUrl) {
    return new AuthenticatorCredentials(username, password, clientId, clientSecret, loginUrl);
  }

  /**
   * Creates {@link AuthenticatorCredentials} instance based on given {@link Configuration}.
   *
   * @param conf hadoop job configuration
   * @return authenticator credentials
   */
  public static AuthenticatorCredentials getAuthenticatorCredentials(Configuration conf) {
    return getAuthenticatorCredentials(conf.get(SalesforceConstants.CONFIG_USERNAME),
                                        conf.get(SalesforceConstants.CONFIG_PASSWORD),
                                        conf.get(SalesforceConstants.CONFIG_CLIENT_ID),
                                        conf.get(SalesforceConstants.CONFIG_CLIENT_SECRET),
                                        conf.get(SalesforceConstants.CONFIG_LOGIN_URL));
  }
}
