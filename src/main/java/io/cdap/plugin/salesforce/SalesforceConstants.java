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
package io.cdap.plugin.salesforce;

import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.function.Function;

/**
 * Constants related to Salesforce and configuration
 */
public class SalesforceConstants {

  public static final String API_VERSION = "53.0";
  public static final String REFERENCE_NAME_DELIMITER = ".";

  public static final String PROPERTY_CONSUMER_KEY = "consumerKey";
  public static final String PROPERTY_CONSUMER_SECRET = "consumerSecret";
  public static final String PROPERTY_USERNAME = "username";
  public static final String PROPERTY_PASSWORD = "password";
  public static final String PLUGIN_NAME = "Salesforce";
  public static final String PROPERTY_SECURITY_TOKEN = "securityToken";
  public static final String PROPERTY_LOGIN_URL = "loginUrl";
  public static final String PROPERTY_OAUTH_INFO = "oAuthInfo";

  public static final String CONFIG_OAUTH_TOKEN = "mapred.salesforce.oauth.token";
  public static final String CONFIG_OAUTH_INSTANCE_URL = "mapred.salesforce.oauth.instance.url";
  public static final String CONFIG_CONSUMER_KEY = "mapred.salesforce.consumer.key";
  public static final String CONFIG_PASSWORD = "mapred.salesforce.password";
  public static final String CONFIG_USERNAME = "mapred.salesforce.user";
  public static final String CONFIG_CONSUMER_SECRET = "mapred.salesforce.consumer.secret";
  public static final String CONFIG_LOGIN_URL = "mapred.salesforce.login.url";

  public static final int RANGE_FILTER_MIN_VALUE = 0;
  public static final int SOQL_MAX_LENGTH = 20000;

  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 30000;
  public static final int DEFAULT_READ_TIMEOUT_SEC = 18000;
  public static final String PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  public static final String PROPERTY_READ_TIMEOUT = "readTimeout";
  public static final String CONFIG_CONNECT_TIMEOUT = "mapred.salesforce.connectTimeout";
  public static final String CONFIG_READ_TIMEOUT = "mapred.salesforce.readTimeout";

  public static final String PROPERTY_PROXY_URL = "proxyUrl";
  public static final String CONFIG_PROXY_URL = "mapred.salesforce.proxyUrl";
  public static final String REGEX_PROXY_URL = "^(?i)(https?)://.*$";

  public static final String PROPERTY_MAX_RETRY_TIME_IN_MINS = "cdap.streaming.maxRetryTimeInMins";
  public static final long DEFAULT_MAX_RETRY_TIME_IN_MINS = 360L;

  public static Function<PluginConfig, Boolean> isOAuthMacroFunction = config -> config.containsMacro(
    PROPERTY_OAUTH_INFO);
}
