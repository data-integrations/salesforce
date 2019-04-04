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

package co.cask.hydrator.salesforce;

/**
 * Constants related to Salesforce and configuration
 */
public class SalesforceConstants {
  public static final String API_VERSION = "45.0";

  public static final String CONFIG_CLIENT_ID = "mapred.salesforce.client.id";
  public static final String CONFIG_PASSWORD = "mapred.salesforce.password";
  public static final String CONFIG_USERNAME = "mapred.salesforce.user";
  public static final String CONFIG_CLIENT_SECRET = "mapred.salesforce.client.secret";
  public static final String CONFIG_LOGIN_URL = "mapred.salesforce.login.url";
  public static final String CONFIG_ERROR_HANDLING = "mapred.salesforce.error.handling";
}
