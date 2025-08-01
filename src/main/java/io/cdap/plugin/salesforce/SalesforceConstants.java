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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Constants related to Salesforce and configuration
 */
public class SalesforceConstants {

  public static final String API_VERSION = "62.0";
  public static final String REFERENCE_NAME_DELIMITER = ".";

  public static final String PROPERTY_CONSUMER_KEY = "consumerKey";
  public static final String PROPERTY_CONSUMER_SECRET = "consumerSecret";
  public static final String PROPERTY_USERNAME = "username";
  public static final String PROPERTY_PASSWORD = "password";
  public static final String PLUGIN_NAME = "Salesforce";
  public static final String PROPERTY_SECURITY_TOKEN = "securityToken";
  public static final String PROPERTY_LOGIN_URL = "loginUrl";
  public static final String PROPERTY_OAUTH_INFO = "oAuthInfo";
  public static final String PROPERTY_INITIAL_RETRY_DURATION = "initialRetryDuration";
  public static final String PROPERTY_MAX_RETRY_DURATION = "maxRetryDuration";
  public static final String PROPERTY_MAX_RETRY_COUNT = "maxRetryCount";
  public static final String PROPERTY_RETRY_REQUIRED = "retryOnBackendError";

  public static final String CONFIG_OAUTH_TOKEN = "mapred.salesforce.oauth.token";
  public static final String CONFIG_OAUTH_INSTANCE_URL = "mapred.salesforce.oauth.instance.url";
  public static final String CONFIG_CONSUMER_KEY = "mapred.salesforce.consumer.key";
  public static final String CONFIG_PASSWORD = "mapred.salesforce.password";
  public static final String CONFIG_USERNAME = "mapred.salesforce.user";
  public static final String CONFIG_CONSUMER_SECRET = "mapred.salesforce.consumer.secret";
  public static final String CONFIG_LOGIN_URL = "mapred.salesforce.login.url";

  public static final int RANGE_FILTER_MIN_VALUE = 0;
  public static final int SOQL_MAX_LENGTH = 20000;

  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 120_000;
  public static final int DEFAULT_READ_TIMEOUT_SEC = 18000;
  public static final String PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  public static final String PROPERTY_READ_TIMEOUT = "readTimeout";
  public static final String CONFIG_CONNECT_TIMEOUT = "mapred.salesforce.connectTimeout";
  public static final String CONFIG_READ_TIMEOUT = "mapred.salesforce.readTimeout";
  public static final String CONFIG_INITIAL_RETRY_DURATION = "mapred.salesforce.initialRetryDuration";
  public static final String CONFIG_MAX_RETRY_DURATION = "mapred.salesforce.maxRetryDuration";
  public static final String CONFIG_MAX_RETRY_COUNT = "mapred.salesforce.maxRetryCount";
  public static final String CONFIG_RETRY_REQUIRED = "mapred.salesforce.retryOnBackendError";

  public static final String PROPERTY_PROXY_URL = "proxyUrl";
  public static final String CONFIG_PROXY_URL = "mapred.salesforce.proxyUrl";
  public static final String REGEX_PROXY_URL = "^(?i)(https?)://.*$";

  public static final String PROPERTY_MAX_RETRY_TIME_IN_MINS = "cdap.streaming.maxRetryTimeInMins";
  public static final long DEFAULT_MAX_RETRY_TIME_IN_MINS = 360L;

  public static Function<PluginConfig, Boolean> isOAuthMacroFunction = config -> config.containsMacro(
    PROPERTY_OAUTH_INFO);
  public static final long DEFAULT_INITIAL_RETRY_DURATION_SECONDS = 5L;

  public static final long DEFAULT_MAX_RETRY_DURATION_SECONDS = 80L;

  public static final int DEFAULT_MAX_RETRY_COUNT = 5;

  // Below is the list of Objects not supported by Bulk API. Attachment, ContentVersion, Document, StaticResource,
  // SControl, EmailCapture, MailmergeTemplate contains binary fields which will cause the batch read to fail.
  public static final Set<String> UNSUPPORTED_BULK_API_OBJECTS = Collections.unmodifiableSet(
    new HashSet<>(Arrays.asList(
      "attachment", "contentversion", "document", "staticresource", "scontrol",
      "emailcapture", "mailmergetemplate", "acceptedeventrelation", "casestatus",
      "contentfolderitem", "contractstatus", "declinedeventrelation",
      "fieldsecurityclassification", "orderstatus", "partnerrole", "recentlyviewed",
      "solutionstatus", "taskpriority", "taskstatus", "undecidedeventrelation",
      "userrecordaccess", "workorderlineitemstatus", "workorderstatus"
    )));

}
