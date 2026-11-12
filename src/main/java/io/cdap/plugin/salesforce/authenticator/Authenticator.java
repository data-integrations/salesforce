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

package io.cdap.plugin.salesforce.authenticator;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.sforce.ws.ConnectorConfig;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Authentication to Salesforce via oauth2
 */
public class Authenticator {
  private static final Gson GSON = new Gson();
  /**
   * Matches an HTML {@code <div ... id="error" ...>...</div>} element returned
   * by Salesforce HTML login/error pages instead of JSON, capturing its inner
   * content across multiple lines (DOTALL) regardless of tag case.
   */
  private static final Pattern HTML_ERROR_PATTERN =
      Pattern.compile("<div\\b[^>]*\\bid=['\"]?error['\"]?[^>]*>(.*?)</div>",
          Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

  /**
   * Authenticates via oauth2 to salesforce and returns a connectorConfig
   * which can be used by salesforce libraries to make a connection.
   *
   * @param credentials information to log in
   * @return ConnectorConfig which can be used to create BulkConnection and PartnerConnection
   */
  public static ConnectorConfig createConnectorConfig(AuthenticatorCredentials credentials) {
    try {
      OAuthInfo oAuthInfo = getOAuthInfo(credentials);
      ConnectorConfig connectorConfig = new ConnectorConfig();
      connectorConfig.setSessionId(oAuthInfo.getAccessToken());
      String apiVersion = SalesforceConstants.API_VERSION;
      String restEndpoint = String.format("%s/services/async/%s", oAuthInfo.getInstanceURL(), apiVersion);
      String serviceEndPoint = String.format("%s/services/Soap/u/%s", oAuthInfo.getInstanceURL(), apiVersion);
      connectorConfig.setRestEndpoint(restEndpoint);
      connectorConfig.setServiceEndpoint(serviceEndPoint);
      // set proxy if proxy server details are available
      if (!Strings.isNullOrEmpty(credentials.getProxyUrl())) {
        URI proxyUrl = new URI(credentials.getProxyUrl());
        connectorConfig.setProxy(proxyUrl.getHost(), proxyUrl.getPort());
      }
      // This should only be false when doing debugging.
      connectorConfig.setCompression(true);
      // Set this to true to see HTTP requests and responses on stdout
      connectorConfig.setTraceMessage(false);
      connectorConfig.setConnectionTimeout(credentials.getConnectTimeout());
      connectorConfig.setReadTimeout(credentials.getReadTimeout());
      return connectorConfig;
    } catch (Exception e) {
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
          String.format("Failed to connect and authenticate to Salesforce: %s", errorMessage), e);
    }
  }

  /**
   * Authenticate via oauth2 to salesforce and return response to auth request.
   *
   * @param credentials information to log in
   * @return AuthResponse response to http request
   */
  public static OAuthInfo getOAuthInfo(AuthenticatorCredentials credentials) throws Exception {
    OAuthInfo oAuthInfo = credentials.getOAuthInfo();
    if (oAuthInfo != null) {
      return oAuthInfo;
    }

    if (credentials.getGrantType() == null) {
      throw new IllegalArgumentException("Grant type cannot be null for OAuth flow to fetch access token.");
    }

    OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
        .connectTimeout(credentials.getConnectTimeout(), TimeUnit.MILLISECONDS);
    if (credentials.getReadTimeout() != null) {
      clientBuilder.readTimeout(credentials.getReadTimeout(), TimeUnit.MILLISECONDS);
    }
    if (!Strings.isNullOrEmpty(credentials.getProxyUrl())) {
      setProxy(credentials, clientBuilder);
    }
    OkHttpClient httpClient = clientBuilder.build();

    FormBody.Builder formBuilder = new FormBody.Builder()
        .add("grant_type", credentials.getGrantType().getType());

    switch (credentials.getGrantType()) {
      case CLIENT_CREDENTIALS:
        formBuilder.add("client_id", credentials.getConsumerKey())
            .add("client_secret", credentials.getConsumerSecret());
        break;
      case PASSWORD:
        formBuilder.add("client_id", credentials.getConsumerKey())
            .add("client_secret", credentials.getConsumerSecret())
            .add("username", credentials.getUsername())
            .add("password", credentials.getPassword());
        break;
    }

    Request authRequest = new Request.Builder()
        .url(credentials.getLoginUrl())
        .post(formBuilder.build())
        .build();

    try (Response httpResponse = httpClient.newCall(authRequest).execute()) {
      int statusCode = httpResponse.code();
      String reason = httpResponse.message();
      String response = httpResponse.body() != null ? httpResponse.body().string() : "";
      AuthResponse authResponse;

      try {
        authResponse = GSON.fromJson(response, AuthResponse.class);
      } catch (JsonSyntaxException e) {
        throw new IllegalStateException(
            String.format(
                "Cannot authenticate to Salesforce (HTTP %d %s). %s",
                statusCode,
                Strings.nullToEmpty(reason),
                extractErrorFromResponse(response)),
            e);
      }

      if (authResponse == null) {
        throw new IllegalStateException(
            String.format(
                "Empty OAuth response from Salesforce (HTTP %d %s).",
                statusCode, Strings.nullToEmpty(reason)));
      }

      if (!Strings.isNullOrEmpty(authResponse.getError())) {
        throw new IllegalArgumentException(
          String.format("Cannot authenticate to Salesforce with given credentials. ServerResponse='%s'",
                        response));
      }

      return new OAuthInfo(authResponse.getAccessToken(), authResponse.getInstanceUrl());
    } finally {
      closeHttpClient(httpClient);
    }
  }

  /**
   * Releases dispatcher executor threads and evicts idle connections for a per-call {@link OkHttpClient}.
   *
   * @param httpClient client instance to clean up
   */
  public static void closeHttpClient(OkHttpClient httpClient) {
    if (httpClient != null) {
      if (httpClient.dispatcher() != null && httpClient.dispatcher().executorService() != null) {
        httpClient.dispatcher().executorService().shutdown();
      }
      if (httpClient.connectionPool() != null) {
        httpClient.connectionPool().evictAll();
      }
    }
  }

  /**
   * Set a proxy to http client based on user input.
   *
   * @param credentials   Credentials contain the proxy server details set in config.
   * @param clientBuilder OkHttpClient.Builder to be used to call salesforce APIs
   */
  public static void setProxy(AuthenticatorCredentials credentials, OkHttpClient.Builder clientBuilder) {
    if (!credentials.getProxyUrl().matches(SalesforceConstants.REGEX_PROXY_URL)) {
      throw new IllegalArgumentException(String.format("Proxy URL format is wrong: %s.", credentials.getProxyUrl()));
    }

    try {
      URI proxyUrl = new URI(credentials.getProxyUrl());
      Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl.getHost(), proxyUrl.getPort()));
      clientBuilder.proxy(proxy);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Cannot set up proxy server call with " +
                                                         "given proxy server details. Error : %s", e.getMessage()), e);
    }
  }

  /**
   * Extracts a concise, human-readable error message from a non-JSON Salesforce
   * OAuth response instead of embedding the entire raw HTML page in the
   * exception message.
   *
   * <p>It searches the response for an HTML error container matching
   * {@link #HTML_ERROR_PATTERN}, strips any nested HTML tags from its inner
   * content, and trims surrounding whitespace. If no non-empty error container
   * is found, it returns a generic authentication error message.
   *
   * <p>Example:
   * <pre>{@code
   * // 1. Input HTML containing an id="error" div:
   * "<div class="loginError" id="error">Error: <span>Enter username.</span></div>"
   * // Returns: "Error: Enter username."
   *
   * // 2. Input HTML without an id="error" div (e.g., 503 maintenance page):
   * "<html><body><h1>503 Service Unavailable</h1></body></html>"
   * // Returns: "Error while authenticating to Salesforce with given credentials."
   * }</pre>
   *
   * @param response raw HTTP response body returned by Salesforce OAuth
   * @return extracted plain-text error message, or a default error message
   */
  private static String extractErrorFromResponse(String response) {
    if (!Strings.isNullOrEmpty(response)) {
      Matcher matcher = HTML_ERROR_PATTERN.matcher(response);
      if (matcher.find()) {
        // Strip any nested HTML tags inside the <div id="error">...</div> block (e.g.,
        // <span>, <b>).
        String error = matcher.group(1).replaceAll("<[^>]+>", "").trim();
        if (!error.isEmpty()) {
          return error;
        }
      }
    }
    return "Error while authenticating to Salesforce with given credentials.";
  }
}
