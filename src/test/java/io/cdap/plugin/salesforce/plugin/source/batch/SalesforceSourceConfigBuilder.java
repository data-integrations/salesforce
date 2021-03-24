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

package io.cdap.plugin.salesforce.plugin.source.batch;

/**
 * Helper class to simplify {@link SalesforceSourceConfig} class creation.
 */
public class SalesforceSourceConfigBuilder {
  private String referenceName;
  private String consumerKey;
  private String consumerSecret;
  private String username;
  private String password;
  private String loginUrl;
  private String query;
  private String sObjectName;
  private String datetimeAfter;
  private String datetimeBefore;
  private String duration;
  private String offset;
  private String schema;
  private String securityToken;
  private Boolean enablePKChunk;
  private Integer chunkSize;

  public SalesforceSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SalesforceSourceConfigBuilder setConsumerKey(String consumerKey) {
    this.consumerKey = consumerKey;
    return this;
  }

  public SalesforceSourceConfigBuilder setConsumerSecret(String consumerSecret) {
    this.consumerSecret = consumerSecret;
    return this;
  }

  public SalesforceSourceConfigBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  public SalesforceSourceConfigBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public SalesforceSourceConfigBuilder setLoginUrl(String loginUrl) {
    this.loginUrl = loginUrl;
    return this;
  }

  public SalesforceSourceConfigBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public SalesforceSourceConfigBuilder setSObjectName(String sObjectName) {
    this.sObjectName = sObjectName;
    return this;
  }

  public SalesforceSourceConfigBuilder setDatetimeAfter(String datetimeAfter) {
    this.datetimeAfter = datetimeAfter;
    return this;
  }

  public SalesforceSourceConfigBuilder setDatetimeBefore(String datetimeBefore) {
    this.datetimeBefore = datetimeBefore;
    return this;
  }

  public SalesforceSourceConfigBuilder setDuration(String duration) {
    this.duration = duration;
    return this;
  }

  public SalesforceSourceConfigBuilder setOffset(String offset) {
    this.offset = offset;
    return this;
  }

  public SalesforceSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public SalesforceSourceConfigBuilder setSecurityToken(String securityToken) {
    this.securityToken = securityToken;
    return this;
  }

  public SalesforceSourceConfigBuilder setEnablePKChunk(Boolean enablePKChunk) {
    this.enablePKChunk = enablePKChunk;
    return this;
  }

  public SalesforceSourceConfigBuilder setChunkSize(Integer chunkSize) {
    this.chunkSize = chunkSize;
    return this;
  }

  public SalesforceSourceConfig build() {
    return new SalesforceSourceConfig(referenceName, consumerKey, consumerSecret, username, password, loginUrl,
                                      query, sObjectName, datetimeAfter, datetimeBefore, duration, offset, schema,
                                      securityToken, null, enablePKChunk, chunkSize);
  }
}
