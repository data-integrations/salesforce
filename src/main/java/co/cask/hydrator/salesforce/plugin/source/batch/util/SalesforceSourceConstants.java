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
package co.cask.hydrator.salesforce.plugin.source.batch.util;

/**
 * Salesforce batch source constants
 */
public class SalesforceSourceConstants {

  public static final String PROPERTY_QUERY = "query";
  public static final String PROPERTY_SOBJECT_NAME = "sObjectName";
  public static final String PROPERTY_DATETIME_FILTER = "datetimeFilter";
  public static final String PROPERTY_DURATION = "duration";
  public static final String PROPERTY_OFFSET = "offset";

  public static final String CONFIG_QUERY = "mapred.salesforce.input.query";

  public static final int DURATION_DEFAULT = 0;
  public static final int OFFSET_DEFAULT = 0;
  public static final int WIDE_QUERY_MAX_BATCH_COUNT = 2000;
}
