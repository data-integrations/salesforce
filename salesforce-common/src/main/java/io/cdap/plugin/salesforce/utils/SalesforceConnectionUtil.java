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
package io.cdap.plugin.salesforce.utils;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.cdap.plugin.salesforce.auth.Authenticator;
import io.cdap.plugin.salesforce.auth.AuthenticatorCredentials;

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
}
