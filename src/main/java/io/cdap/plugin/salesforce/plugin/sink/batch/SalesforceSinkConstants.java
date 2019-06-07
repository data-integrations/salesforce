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
package io.cdap.plugin.salesforce.plugin.sink.batch;

/**
 * Constants related to Salesforce Batch Sink
 */
public class SalesforceSinkConstants {
  public static final String CONFIG_SOBJECT = "mapred.salesforce.sobject.name";
  public static final String CONFIG_OPERATION = "mapred.salesforce.operation.type";
  public static final String CONFIG_EXTERNAL_ID_FIELD = "mapred.salesforce.external.id";
  public static final String CONFIG_ERROR_HANDLING = "mapred.salesforce.error.handling";
  public static final String CONFIG_JOB_ID = "mapred.salesforce.job.id";
  public static final String CONFIG_MAX_BYTES_PER_BATCH = "mapred.salesforce.max.bytes.per.batch";
  public static final String CONFIG_MAX_RECORDS_PER_BATCH = "mapred.salesforce.max.records.per.batch";
}
