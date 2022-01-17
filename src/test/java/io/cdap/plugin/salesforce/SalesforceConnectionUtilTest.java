/*
 * Copyright © 2022 Cask Data, Inc.
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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SalesforceConnectionUtilTest {

  @Mock
  SalesforceConnectionUtil salesforceConnectionUtil;

  @Test
  public void testGetPartnerConnection() throws ConnectionException {
    PartnerConnection actualPartnerConnection = SalesforceConnectionUtil
      .getPartnerConnection(new AuthenticatorCredentials(new OAuthInfo("token",
                                                                       "https://example.org/example")));
    Class<JdkHttpTransport> expectedTransport = JdkHttpTransport.class;
    ConnectorConfig config = actualPartnerConnection.getConfig();
    Assert.assertSame(expectedTransport, config.getTransport());
    Assert.assertEquals("token", config.getSessionId());
    Assert.assertEquals("https://example.org/example/services/Soap/u/53.0", config.getServiceEndpoint());
    Assert.assertEquals("https://example.org/example/services/async/53.0", config.getRestEndpoint());
    Assert.assertEquals("token", actualPartnerConnection.getSessionHeader().getSessionId());
  }

  @Test
  public void testGetAuthenticatorCredentials() {
    AuthenticatorCredentials actualAuthenticatorCredentials = SalesforceConnectionUtil.getAuthenticatorCredentials(
      "username", "password", "Consumer Key", "Consumer Secret",
      "https://example.org/example");
    Assert.assertEquals("Consumer Key", actualAuthenticatorCredentials.getConsumerKey());
    Assert.assertEquals("username", actualAuthenticatorCredentials.getUsername());
    Assert.assertEquals("password", actualAuthenticatorCredentials.getPassword());
    Assert.assertNull(actualAuthenticatorCredentials.getOAuthInfo());
    Assert.assertEquals("https://example.org/example", actualAuthenticatorCredentials.getLoginUrl());
    Assert.assertEquals("Consumer Secret", actualAuthenticatorCredentials.getConsumerSecret());
  }
}

