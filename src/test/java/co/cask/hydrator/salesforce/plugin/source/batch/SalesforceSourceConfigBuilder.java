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

package co.cask.hydrator.salesforce.plugin.source.batch;

/**
 * Helper class to simplify {@link SalesforceSourceConfig} class creation.
 */
public class SalesforceSourceConfigBuilder {
  private String referenceName;
  private String clientId;
  private String clientSecret;
  private String username;
  private String password;
  private String loginUrl;
  private String errorHandling;
  private String query;
  private String sObjectName;
  private String datetimeFilter;
  private Integer duration;
  private Integer offset;
  private String schema;

  public SalesforceSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SalesforceSourceConfigBuilder setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public SalesforceSourceConfigBuilder setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
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

  public SalesforceSourceConfigBuilder setErrorHandling(String errorHandling) {
    this.errorHandling = errorHandling;
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

  public SalesforceSourceConfigBuilder setDatetimeFilter(String datetimeFilter) {
    this.datetimeFilter = datetimeFilter;
    return this;
  }

  public SalesforceSourceConfigBuilder setDuration(Integer duration) {
    this.duration = duration;
    return this;
  }

  public SalesforceSourceConfigBuilder setOffset(Integer offset) {
    this.offset = offset;
    return this;
  }

  public SalesforceSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public SalesforceSourceConfig build() {
    return new SalesforceSourceConfig(referenceName, clientId, clientSecret, username, password, loginUrl,
                                      errorHandling, query, sObjectName, datetimeFilter, duration, offset, schema);
  }
}
