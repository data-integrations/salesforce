/*
 * Copyright © 2026 Cask Data, Inc.
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

import okhttp3.OkHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Proxy;

/**
 * Unit tests for {@link Authenticator}.
 */
public class AuthenticatorTest {

  @Test
  public void setProxy_httpWithoutPort_defaultsToPort80() {
    AuthenticatorCredentials credentials = AuthenticatorCredentials.getBuilder("key", "secret", "https://login.sf.com")
        .setProxyUrl("http://myproxy.internal")
        .build();
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    Authenticator.setProxy(credentials, builder);
    Proxy proxy = builder.build().proxy();

    Assert.assertNotNull(proxy);
    InetSocketAddress address = (InetSocketAddress) proxy.address();
    Assert.assertEquals("myproxy.internal", address.getHostString());
    Assert.assertEquals(80, address.getPort());
  }

  @Test
  public void setProxy_httpsWithoutPort_defaultsToPort443() {
    AuthenticatorCredentials credentials = AuthenticatorCredentials.getBuilder("key", "secret", "https://login.sf.com")
        .setProxyUrl("https://myproxy.internal")
        .build();
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    Authenticator.setProxy(credentials, builder);
    Proxy proxy = builder.build().proxy();

    Assert.assertNotNull(proxy);
    InetSocketAddress address = (InetSocketAddress) proxy.address();
    Assert.assertEquals("myproxy.internal", address.getHostString());
    Assert.assertEquals(443, address.getPort());
  }

  @Test
  public void setProxy_explicitPort_usesConfiguredPort() {
    AuthenticatorCredentials credentials = AuthenticatorCredentials.getBuilder("key", "secret", "https://login.sf.com")
        .setProxyUrl("http://myproxy.internal:8443")
        .build();
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    Authenticator.setProxy(credentials, builder);
    Proxy proxy = builder.build().proxy();

    Assert.assertNotNull(proxy);
    InetSocketAddress address = (InetSocketAddress) proxy.address();
    Assert.assertEquals("myproxy.internal", address.getHostString());
    Assert.assertEquals(8443, address.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setProxy_missingHost_throwsIllegalArgumentException() {
    AuthenticatorCredentials credentials = AuthenticatorCredentials.getBuilder("key", "secret", "https://login.sf.com")
        .setProxyUrl("http://")
        .build();
    Authenticator.setProxy(credentials, new OkHttpClient.Builder());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getOAuthInfo_nullLoginUrl_throwsIllegalArgumentException() throws Exception {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(
        null, null, null, "key", "secret", null,
        AuthenticatorCredentials.GrantType.CLIENT_CREDENTIALS, 1000, 1000, null, null, null, null, null);
    Authenticator.getOAuthInfo(credentials);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getOAuthInfo_nullConsumerKey_throwsIllegalArgumentException() throws Exception {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(
        null, null, null, null, "secret", "https://login.salesforce.com/services/oauth2/token",
        AuthenticatorCredentials.GrantType.CLIENT_CREDENTIALS, 1000, 1000, null, null, null, null, null);
    Authenticator.getOAuthInfo(credentials);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getOAuthInfo_passwordGrantWithNullUsername_throwsIllegalArgumentException() throws Exception {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(
        null, null, "pass", "key", "secret", "https://login.salesforce.com/services/oauth2/token",
        AuthenticatorCredentials.GrantType.PASSWORD, 1000, 1000, null, null, null, null, null);
    Authenticator.getOAuthInfo(credentials);
  }
}
