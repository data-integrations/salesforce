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

package io.cdap.plugin.salesforce.plugin.marketing;

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Hadoop output format for writing to Salesforce Marketing Cloud.
 */
public class DataExtensionOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  public static final String CLIENT_ID = "cdap.sfmc.client.id";
  public static final String CLIENT_SECRET = "cdap.sfmc.client.secret";
  public static final String AUTH_ENDPOINT = "cdap.sfmc.auth.endpoint";
  public static final String SOAP_ENDPOINT = "cdap.sfmc.soap.endpoint";
  public static final String DATA_EXTENSION_KEY = "cdap.sfmc.data.extension.key";
  public static final String MAX_BATCH_SIZE = "cdap.sfmc.max.batch.size";
  public static final String FAIL_ON_ERROR = "cdap.sfmc.fail.on.error";
  public static final String OPERATION = "cdap.sfmc.operation";

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    String clientId = getOrError(conf, CLIENT_ID);
    String clientSecret = getOrError(conf, CLIENT_SECRET);
    String authEndpoint = getOrError(conf, AUTH_ENDPOINT);
    String soapEndpoint = getOrError(conf, SOAP_ENDPOINT);
    String dataExtensionKey = getOrError(conf, DATA_EXTENSION_KEY);
    Operation operation = Operation.valueOf(getOrError(conf, OPERATION));
    int maxBatchSize = Integer.parseInt(getOrError(conf, MAX_BATCH_SIZE));
    boolean failOnError = Boolean.parseBoolean(getOrError(conf, FAIL_ON_ERROR));
    try {
      DataExtensionClient client = DataExtensionClient.create(dataExtensionKey, clientId, clientSecret,
                                                              authEndpoint, soapEndpoint);
      return new DataExtensionRecordWriter(client, operation, maxBatchSize, failOnError);
    } catch (ETSdkException e) {
      throw new IOException("Unable to create Salesforce Marketing Cloud client.", e);
    }
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    // no-op
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) {
        // no-op
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) {
        // no-op
      }
    };
  }

  private String getOrError(Configuration conf, String key) {
    String val = conf.get(key);
    if (val == null) {
      throw new IllegalStateException("Missing required value for " + key);
    }
    return val;
  }
}
