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
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.transport.JdkHttpTransport;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(MockitoJUnitRunner.class)
public class SalesforceConnectionUtilTest {

  @Mock
  SalesforceConnectionUtil salesforceConnectionUtil;

  @Test
  public void testGetPartnerConnection() throws ConnectionException {
    PartnerConnection actualPartnerConnection = SalesforceConnectionUtil
      .getPartnerConnection(new AuthenticatorCredentials(new OAuthInfo("token", "https://example.org/example")));
    Class<JdkHttpTransport> expectedTransport = JdkHttpTransport.class;
    ConnectorConfig config = actualPartnerConnection.getConfig();
    assertSame(expectedTransport, config.getTransport());
    assertEquals("token", config.getSessionId());
    assertEquals("https://example.org/example/services/Soap/u/53.0", config.getServiceEndpoint());
    assertEquals("https://example.org/example/services/async/53.0", config.getRestEndpoint());
    assertEquals("token", actualPartnerConnection.getSessionHeader().getSessionId());
  }

  @Test
  public void testGetAuthenticatorCredentials() {
    AuthenticatorCredentials actualAuthenticatorCredentials = SalesforceConnectionUtil.getAuthenticatorCredentials(
      "username", "password", "Consumer Key", "Consumer Secret", "https://example.org/example");
    assertEquals("Consumer Key", actualAuthenticatorCredentials.getConsumerKey());
    assertEquals("username", actualAuthenticatorCredentials.getUsername());
    assertEquals("password", actualAuthenticatorCredentials.getPassword());
    assertNull(actualAuthenticatorCredentials.getOAuthInfo());
    assertEquals("https://example.org/example", actualAuthenticatorCredentials.getLoginUrl());
    assertEquals("Consumer Secret", actualAuthenticatorCredentials.getConsumerSecret());
  }
}

