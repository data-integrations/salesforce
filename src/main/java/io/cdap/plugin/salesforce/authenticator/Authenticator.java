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
import com.sforce.ws.ConnectorConfig;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Authentication to Salesforce via oauth2
 */
public class Authenticator {
  private static final Gson GSON = new Gson();

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

    SslContextFactory sslContextFactory = new SslContextFactory();
    HttpClient httpClient = new HttpClient(sslContextFactory);
    httpClient.setConnectTimeout(credentials.getConnectTimeout());
    if (!Strings.isNullOrEmpty(credentials.getProxyUrl())) {
      setProxy(credentials, httpClient);
    }
    try {
      httpClient.start();
      String response = httpClient.POST(credentials.getLoginUrl()).param("grant_type", "password")
        .param("client_id", credentials.getConsumerKey())
        .param("client_secret", credentials.getConsumerSecret())
        .param("username", credentials.getUsername())
        .param("password", credentials.getPassword()).send().getContentAsString();

      AuthResponse authResponse = GSON.fromJson(response, AuthResponse.class);

      if (!Strings.isNullOrEmpty(authResponse.getError())) {
        throw new IllegalArgumentException(
          String.format("Cannot authenticate to Salesforce with given credentials. ServerResponse='%s'",
                        response));
      }

      return new OAuthInfo(authResponse.getAccessToken(), authResponse.getInstanceUrl());
    } finally {
      httpClient.stop();
    }
  }

  /**
   * Set a proxy to http client based on user input.
   *
   * @param credentials Credentials contain the proxy server details set in config.
   * @param httpClient  httpClient to be used to call salesforce APIs
   */
  public static void setProxy(AuthenticatorCredentials credentials, HttpClient httpClient) {
    if (!credentials.getProxyUrl().matches(SalesforceConstants.REGEX_PROXY_URL)) {
      throw new IllegalArgumentException(String.format("Proxy URL format is wrong: %s.", credentials.getProxyUrl()));
    }

    try {
      URI proxyUrl = new URI(credentials.getProxyUrl());
      ProxyConfiguration proxyConfig = httpClient.getProxyConfiguration();
      HttpProxy proxy = new HttpProxy(proxyUrl.getHost(), proxyUrl.getPort());
      proxyConfig.getProxies().add(proxy);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Cannot set up proxy server call with " +
                                                         "given proxy server details. Error : %s", e.getMessage()), e);
    }
  }
}
