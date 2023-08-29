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
package io.cdap.plugin.salesforce;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.IApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorInfo;
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
   * Creates {@link AuthenticatorCredentials} instance based on given {@link Configuration}.
   *
   * @param conf hadoop job configuration
   * @return authenticator credentials
   */
  public static AuthenticatorCredentials getAuthenticatorCredentials(Configuration conf) {
    String oAuthToken = conf.get(SalesforceConstants.CONFIG_OAUTH_TOKEN);
    String instanceURL = conf.get(SalesforceConstants.CONFIG_OAUTH_INSTANCE_URL);
    Integer connectTimeout = SalesforceConstants.DEFAULT_CONNECTION_TIMEOUT_MS;
    if (conf.get(SalesforceConstants.CONFIG_CONNECT_TIMEOUT) != null) {
      connectTimeout = Integer.parseInt(conf.get(SalesforceConstants.CONFIG_CONNECT_TIMEOUT));
    }
    String proxyUrl = conf.get(SalesforceConstants.CONFIG_PROXY_URL);
    if (oAuthToken != null && instanceURL != null) {
      return new AuthenticatorCredentials(new OAuthInfo(oAuthToken, instanceURL), connectTimeout, proxyUrl);
    }

    return new AuthenticatorCredentials(conf.get(SalesforceConstants.CONFIG_USERNAME),
                                        conf.get(SalesforceConstants.CONFIG_PASSWORD),
                                        conf.get(SalesforceConstants.CONFIG_CONSUMER_KEY),
                                        conf.get(SalesforceConstants.CONFIG_CONSUMER_SECRET),
                                        conf.get(SalesforceConstants.CONFIG_LOGIN_URL),
                                        connectTimeout, proxyUrl);
  }

  /**
   * @param e Exception thrown from salesforce APIs
   * @return  error message sent by APIs.
   */
  public static String getSalesforceErrorMessageFromException(Exception e) {
    if (e instanceof IApiFault) {
      return ((IApiFault) e).getExceptionMessage();
    } else {
      return e.getMessage();
    }
  }
  /**
   *
   * @param config     SalesforceConnectorConfig from where credentials can be taken
   * @param collector  FailureCollector
   * @return           OAuthInfo which contains Access Token and login URL.
   */
  public static OAuthInfo getOAuthInfo(SalesforceConnectorInfo config, FailureCollector collector) {
    if (!config.canAttemptToEstablishConnection()) {
      return null;
    }
    OAuthInfo oAuthInfo = null;
    try {
      oAuthInfo = Authenticator.getOAuthInfo(config.getAuthenticatorCredentials());
    } catch (Exception e) {
      String message = getSalesforceErrorMessageFromException(e);
      collector.addFailure("Error encountered while establishing connection: " + message,
                           "Please verify authentication properties are provided correctly")
        .withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }
    return oAuthInfo;
  }
}
