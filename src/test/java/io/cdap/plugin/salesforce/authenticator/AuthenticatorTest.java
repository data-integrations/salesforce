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

import com.google.common.base.Strings;
import okhttp3.OkHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

  @Test
  public void getOAuthInfo_readTimeout_throwsExecutionException() throws Exception {
    CountDownLatch releaseServer = new CountDownLatch(1);
    try (ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      Thread serverThread = new Thread(() -> {
        try (Socket ignored = serverSocket.accept()) {
          releaseServer.await(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // Expected when client times out and test closes socket.
        }
      });
      serverThread.setDaemon(true);
      serverThread.start();

      String loginUrl = "http://127.0.0.1:" + serverSocket.getLocalPort() + "/services/oauth2/token";
      AuthenticatorCredentials credentials = new AuthenticatorCredentials(
          null, null, null, "key", "secret", loginUrl,
          AuthenticatorCredentials.GrantType.CLIENT_CREDENTIALS, 5000, 200, null, null, null, null, null);

      try {
        Authenticator.getOAuthInfo(credentials);
        Assert.fail("Expected ExecutionException wrapping SocketTimeoutException");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof SocketTimeoutException);
        Assert.assertFalse(Strings.isNullOrEmpty(e.getMessage()));
        Assert.assertTrue(e.getMessage().contains("java.net.SocketTimeoutException"));
      } finally {
        releaseServer.countDown();
      }
    }
  }

  @Test
  public void getOAuthInfo_oauthErrorJson_throwsIllegalArgumentException() throws Exception {
    String body = "{\"error\":\"invalid_client_id\",\"error_description\":\"client identifier invalid\"}";
    try (ServerSocket serverSocket = startSingleResponseServer(400, "Bad Request", "application/json", body)) {
      String loginUrl = "http://127.0.0.1:" + serverSocket.getLocalPort() + "/services/oauth2/token";
      AuthenticatorCredentials credentials = new AuthenticatorCredentials(
          null, null, null, "key", "secret", loginUrl,
          AuthenticatorCredentials.GrantType.CLIENT_CREDENTIALS, 5000, 5000, null, null, null, null, null);

      try {
        Authenticator.getOAuthInfo(credentials);
        Assert.fail("Expected IllegalArgumentException on OAuth error payload");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("invalid_client_id"));
        Assert.assertTrue(e.getMessage().contains("client identifier invalid"));
      }
    }
  }

  @Test
  public void getOAuthInfo_htmlErrorResponse_throwsIllegalStateException() throws Exception {
    String body = "<html><body><div id=\"error\">Service Unavailable</div></body></html>";
    try (ServerSocket serverSocket = startSingleResponseServer(503, "Service Unavailable", "text/html", body)) {
      String loginUrl = "http://127.0.0.1:" + serverSocket.getLocalPort() + "/services/oauth2/token";
      AuthenticatorCredentials credentials = new AuthenticatorCredentials(
          null, null, null, "key", "secret", loginUrl,
          AuthenticatorCredentials.GrantType.CLIENT_CREDENTIALS, 5000, 5000, null, null, null, null, null);

      try {
        Authenticator.getOAuthInfo(credentials);
        Assert.fail("Expected IllegalStateException on non-JSON error payload");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("HTTP 503"));
        Assert.assertTrue(e.getMessage().contains("Service Unavailable"));
      }
    }
  }

  private static ServerSocket startSingleResponseServer(
      int statusCode, String reason, String contentType, String body) throws Exception {
    ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
    String headers = "HTTP/1.1 " + statusCode + " " + reason + "\r\n"
        + "Content-Type: " + contentType + "\r\n"
        + "Content-Length: " + bodyBytes.length + "\r\n"
        + "Connection: close\r\n\r\n";
    Thread serverThread = new Thread(() -> {
      try (Socket socket = serverSocket.accept()) {
        socket.setSoTimeout(2000);
        InputStream in = socket.getInputStream();
        byte[] buf = new byte[1024];
        in.read(buf);
        OutputStream out = socket.getOutputStream();
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bodyBytes);
        out.flush();
      } catch (Exception ignored) {
        // Ignore teardown exceptions.
      }
    });
    serverThread.setDaemon(true);
    serverThread.start();
    return serverSocket;
  }
}
