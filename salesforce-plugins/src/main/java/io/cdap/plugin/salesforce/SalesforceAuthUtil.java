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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.auth.Authenticator;
import io.cdap.plugin.salesforce.auth.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorInfo;
import io.cdap.plugin.salesforce.utils.SalesforceErrorUtil;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility class for Salesforce authentication functionality.
 */
public class SalesforceAuthUtil {
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
    Integer readTimeout = SalesforceConstants.DEFAULT_READ_TIMEOUT_SEC * 1000;
    if (conf.get(SalesforceConstants.CONFIG_READ_TIMEOUT) != null) {
      readTimeout = Integer.parseInt(conf.get(SalesforceConstants.CONFIG_READ_TIMEOUT));
    }
    String proxyUrl = conf.get(SalesforceConstants.CONFIG_PROXY_URL);
    if (oAuthToken != null && instanceURL != null) {
      return new AuthenticatorCredentials(new OAuthInfo(oAuthToken, instanceURL), connectTimeout, readTimeout,
                                          proxyUrl);
    }

    return new AuthenticatorCredentials(conf.get(SalesforceConstants.CONFIG_USERNAME),
                                        conf.get(SalesforceConstants.CONFIG_PASSWORD),
                                        conf.get(SalesforceConstants.CONFIG_CONSUMER_KEY),
                                        conf.get(SalesforceConstants.CONFIG_CONSUMER_SECRET),
                                        conf.get(SalesforceConstants.CONFIG_LOGIN_URL),
                                        connectTimeout, readTimeout, proxyUrl);
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
      String message = SalesforceErrorUtil.getSalesforceErrorMessageFromException(e);
      collector.addFailure("Error encountered while establishing connection: " + message,
                           "Please verify authentication properties are provided correctly")
        .withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }
    return oAuthInfo;
  }
}
