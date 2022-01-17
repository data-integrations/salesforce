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
package io.cdap.plugin.salesforce.authenticator;

import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.junit.Assert;
import org.junit.Test;

public class AuthenticatorCredentialsTest {
  @Test
  public void testCredentials() {
    AuthenticatorCredentials actualAuthenticatorCredentials = new AuthenticatorCredentials("username", "password",
                                                                                           "Consumer Key", "Consumer Secret", "https://example.org/example");

    Assert.assertEquals("Consumer Key", actualAuthenticatorCredentials.getConsumerKey());
    Assert.assertEquals("Consumer Secret", actualAuthenticatorCredentials.getConsumerSecret());
    Assert.assertEquals("https://example.org/example", actualAuthenticatorCredentials.getLoginUrl());
    Assert.assertNull(actualAuthenticatorCredentials.getOAuthInfo());
    Assert.assertEquals("password", actualAuthenticatorCredentials.getPassword());
    Assert.assertEquals("username", actualAuthenticatorCredentials.getUsername());
  }

  @Test
  public void testCredentialsWithOauthInfoNull() {
    OAuthInfo oAuthInfo = new OAuthInfo("token", "https://example.org/example");
    AuthenticatorCredentials actualAuthenticatorCredentials = new AuthenticatorCredentials(oAuthInfo);
    Assert.assertNotNull(actualAuthenticatorCredentials.getOAuthInfo());
  }
}
