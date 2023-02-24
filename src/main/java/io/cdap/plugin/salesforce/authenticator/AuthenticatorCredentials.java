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
  private final Integer connectTimeout;

  public AuthenticatorCredentials(OAuthInfo oAuthInfo, Integer connectTimeout) {
    this(Objects.requireNonNull(oAuthInfo), null, null, null, null, null, connectTimeout);
  }

  public AuthenticatorCredentials(String username, String password,
                                  String consumerKey, String consumerSecret, String loginUrl, Integer connectTimeout) {
    this(null, Objects.requireNonNull(username), Objects.requireNonNull(password), Objects.requireNonNull(consumerKey),
         Objects.requireNonNull(consumerSecret), Objects.requireNonNull(loginUrl),
            Objects.requireNonNull(connectTimeout));
  }

  public AuthenticatorCredentials(@Nullable OAuthInfo oAuthInfo,
                                  @Nullable String username,
                                  @Nullable String password,
                                  @Nullable String consumerKey,
                                  @Nullable String consumerSecret,
                                  @Nullable String loginUrl,
                                  @Nullable Integer connectTimeout) {
    this.oAuthInfo = oAuthInfo;
    this.username = username;
    this.password = password;
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.loginUrl = loginUrl;
    this.connectTimeout = connectTimeout;
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
  public Integer getConnectTimeout() {
    return connectTimeout;
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
      Objects.equals(loginUrl, that.loginUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password, consumerKey, consumerSecret, loginUrl);
  }
}
