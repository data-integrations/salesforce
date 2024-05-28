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

import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Stores information to connect to salesforce via oauth2
 */
public class AuthenticatorCredentials implements Serializable {

  private final OAuthInfo oAuthInfo;
  private final String username;
  private final String password;
  private final String consumerKey;
  private final String consumerSecret;
  private final String loginUrl;
  private final GrantType grantType;
  private final Integer connectTimeout;
  private final Integer readTimeout;
  private final String proxyUrl;

  public AuthenticatorCredentials(@Nullable OAuthInfo oAuthInfo,
                                   @Nullable String username,
                                   @Nullable String password,
                                   @Nullable String consumerKey,
                                   @Nullable String consumerSecret,
                                   @Nullable String loginUrl,
                                   @Nullable GrantType grantType,
                                   @Nullable Integer connectTimeout,
                                   @Nullable Integer readTimeout,
                                   @Nullable String proxyUrl) {
    this.oAuthInfo = oAuthInfo;
    this.username = username;
    this.password = password;
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.loginUrl = loginUrl;
    this.grantType = grantType;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.proxyUrl = proxyUrl;
  }

  @Nullable
  public OAuthInfo getOAuthInfo() {
    return oAuthInfo;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public String getConsumerKey() {
    return consumerKey;
  }

  @Nullable
  public String getConsumerSecret() {
    return consumerSecret;
  }

  @Nullable
  public String getLoginUrl() {
    return loginUrl;
  }

  @Nullable
  public GrantType getGrantType() {
    return grantType;
  }

  @Nullable
  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public Integer getReadTimeout() {
    return readTimeout;
  }

  @Nullable
  public String getProxyUrl() {
    return proxyUrl;
  }

  /**
   * Builder for {@link AuthenticatorCredentials} with credentials for username-password OAuth flow, where
   * Salesforce OAuth {@link GrantType} is set as "password".
   */
  public static Builder getBuilder(String username, String password,
                                   String consumerKey, String consumerSecret, String loginUrl) {
    return new Builder(
            Objects.requireNonNull(username), Objects.requireNonNull(password),
            Objects.requireNonNull(consumerKey), Objects.requireNonNull(consumerSecret),
            Objects.requireNonNull(loginUrl)
    );
  }

  /**
   * Builder for {@link AuthenticatorCredentials} with credentials for client-credentials OAuth flow,
   * where Salesforce OAuth {@link GrantType} is set as "client-credentials".
   */
  public static Builder getBuilder(String consumerKey, String consumerSecret, String loginUrl) {
    return new Builder(
            Objects.requireNonNull(consumerKey), Objects.requireNonNull(consumerSecret),
            Objects.requireNonNull(loginUrl)
    );
  }

  /**
   * Builder for {@link AuthenticatorCredentials} with already fetched {@link OAuthInfo}.
   */
  public static Builder getBuilder(OAuthInfo oAuthInfo) {
    return new Builder(Objects.requireNonNull(oAuthInfo));
  }

  /**
   * Create an instance of {@link AuthenticatorCredentials} from given set of parameters. Uses "password" grant type
   * if username and password are non-null. Uses client-credentials grant type otherwise.
   */
  public static AuthenticatorCredentials fromParameters(String username, String password,
                                                        String consumerKey, String consumerSecret, String loginUrl,
                                                        Integer connectTimeout, Integer readTimeout, String proxyUrl) {
    AuthenticatorCredentials.Builder builder;
    if (username != null && password != null) {
      builder = AuthenticatorCredentials.getBuilder(username, password, consumerKey, consumerSecret, loginUrl);
    } else {
      builder = AuthenticatorCredentials.getBuilder(consumerKey, consumerSecret, loginUrl);
    }
    return builder.setConnectTimeout(connectTimeout).setReadTimeout(readTimeout).setProxyUrl(proxyUrl).build();
  }

  /**
   * Create an instance of {@link AuthenticatorCredentials} from given {@link OAuthInfo} and parameters.
   */
  public static AuthenticatorCredentials fromParameters(OAuthInfo oAuthInfo,
                                                        Integer connectTimeout, Integer readTimeout, String proxyUrl) {
    return AuthenticatorCredentials.getBuilder(oAuthInfo)
            .setConnectTimeout(connectTimeout).setReadTimeout(readTimeout).setProxyUrl(proxyUrl).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuthenticatorCredentials that = (AuthenticatorCredentials) o;

    return Objects.equals(username, that.username) &&
      Objects.equals(password, that.password) &&
      Objects.equals(consumerKey, that.consumerKey) &&
      Objects.equals(consumerSecret, that.consumerSecret) &&
      Objects.equals(loginUrl, that.loginUrl) &&
      Objects.equals(connectTimeout, that.connectTimeout) &&
      Objects.equals(readTimeout, that.readTimeout) &&
      Objects.equals(proxyUrl, that.proxyUrl) &&
      Objects.equals(grantType, that.grantType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password, consumerKey, consumerSecret, loginUrl, connectTimeout, readTimeout,
                        proxyUrl, grantType);
  }

  /**
   * Builder class for creating an instance of {@link AuthenticatorCredentials}.
   */
  public static final class Builder {
    private OAuthInfo oAuthInfo;
    private String username;
    private String password;
    private String consumerKey;
    private String consumerSecret;
    private String loginUrl;
    private GrantType grantType;
    private Integer connectTimeout;
    private Integer readTimeout;
    private String proxyUrl;

    /**
     * Create an instance of {@link AuthenticatorCredentials} with credentials for username-password OAuth flow, where
     * Salesforce OAuth {@link GrantType} is set as "password".
     */
    public Builder(@Nonnull String username, @Nonnull String password,
                   @Nonnull String consumerKey, @Nonnull String consumerSecret, @Nonnull String loginUrl) {
      this.username = username;
      this.password = password;
      this.consumerKey = consumerKey;
      this.consumerSecret = consumerSecret;
      this.loginUrl = loginUrl;
      this.grantType = GrantType.PASSWORD;
    }

    /**
     * Create an instance of {@link AuthenticatorCredentials} with credentials for client-credentials OAuth flow,
     * where Salesforce OAuth {@link GrantType} is set as "client-credentials".
     */
    public Builder(@Nonnull String consumerKey, @Nonnull String consumerSecret, @Nonnull String loginUrl) {
      this.consumerKey = consumerKey;
      this.consumerSecret = consumerSecret;
      this.loginUrl = loginUrl;
      this.grantType = GrantType.CLIENT_CREDENTIALS;
    }

    /**
     * Create an instance of {@link AuthenticatorCredentials} with already fetched {@link OAuthInfo}.
     */
    public Builder(@Nonnull OAuthInfo oAuthInfo) {
      this.oAuthInfo = oAuthInfo;
    }

    public Builder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder setReadTimeout(Integer readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public Builder setProxyUrl(String proxyUrl) {
      this.proxyUrl = proxyUrl;
      return this;
    }

    public AuthenticatorCredentials build() {
      return new AuthenticatorCredentials(oAuthInfo, username, password, consumerKey, consumerSecret,
              loginUrl, grantType, connectTimeout, readTimeout, proxyUrl);
    }
  }

  /**
   * Represents the grant type parameter for OAuth
   */
  public enum GrantType {
    PASSWORD("password"),
    CLIENT_CREDENTIALS("client_credentials");

    private final String type;

    GrantType(String type) {
      this.type = type;
    }

    public String getType() {
      return type;
    }
  }
}
