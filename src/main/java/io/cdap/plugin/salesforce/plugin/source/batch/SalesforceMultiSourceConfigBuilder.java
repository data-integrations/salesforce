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
package io.cdap.plugin.salesforce.plugin.source.batch;

import io.cdap.plugin.salesforce.plugin.OAuthInfo;

/**
 * Helper class to simplify {@link SalesforceMultiSourceConfig} class creation.
 */
public class SalesforceMultiSourceConfigBuilder {

  private String referenceName;
  private String consumerKey;
  private String consumerSecret;
  private String username;
  private String password;
  private String loginUrl;
  private String datetimeAfter;
  private String datetimeBefore;
  private String duration;
  private String offset;
  private String whiteList;
  private String blackList;
  private String sObjectNameField;
  private String securityToken;
  private OAuthInfo oAuthInfo;
  private String operation;
  private Integer connectTimeout;
  private Integer readTimeout;
  private String proxyUrl;

  public SalesforceMultiSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setConsumerKey(String consumerKey) {
    this.consumerKey = consumerKey;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setConsumerSecret(String consumerSecret) {
    this.consumerSecret = consumerSecret;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setLoginUrl(String loginUrl) {
    this.loginUrl = loginUrl;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setDatetimeAfter(String datetimeAfter) {
    this.datetimeAfter = datetimeAfter;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setDatetimeBefore(String datetimeBefore) {
    this.datetimeBefore = datetimeBefore;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setDuration(String duration) {
    this.duration = duration;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setOffset(String offset) {
    this.offset = offset;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setWhiteList(String whiteList) {
    this.whiteList = whiteList;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setBlackList(String blackList) {
    this.blackList = blackList;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setsObjectNameField(String sObjectNameField) {
    this.sObjectNameField = sObjectNameField;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setSecurityToken(String securityToken) {
    this.securityToken = securityToken;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setoAuthInfo(OAuthInfo oAuthInfo) {
    this.oAuthInfo = oAuthInfo;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setOperation(String operation) {
    this.operation = operation;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setConnectTimeout(Integer connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setReadTimeout(Integer readTimeout) {
    this.readTimeout = readTimeout;
    return this;
  }

  public SalesforceMultiSourceConfigBuilder setProxyUrl(String proxyUrl) {
    this.proxyUrl = proxyUrl;
    return this;
  }

  public SalesforceMultiSourceConfig build() {
    return new SalesforceMultiSourceConfig(referenceName, consumerKey, consumerSecret, username, password, loginUrl,
                                           connectTimeout, readTimeout, datetimeAfter, datetimeBefore, duration, offset,
                                           whiteList,
                                           blackList, sObjectNameField, securityToken, oAuthInfo, operation, proxyUrl);
  }
}
