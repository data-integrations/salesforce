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

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * An OutputFormat that sends the output of a Hadoop job to the Salesforce record writer, also
 * it defines the output committer.
 */
public class SalesforceOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
    throws IOException {

    try {
      return new SalesforceRecordWriter(taskAttemptContext);
    } catch (AsyncApiException e) {
      throw new RuntimeException(
          String.format("Failed to initialize a writer to write to Salesforce: ", e.getMessage()),
          e);
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    //no-op
  }

  /**
   * Used to start Salesforce job when Mapreduce job is started,
   * and to close Salesforce job when Mapreduce job is finished.
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {
        // we cannot set the job id in the conf here since this happens in driver so the executor will not have
        // the value
      }

      @Override
      public void commitJob(JobContext jobContext) {
        Configuration conf = jobContext.getConfiguration();

        AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);

        try {
          BulkConnection bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
          String jobId = conf.get(SalesforceSinkConstants.CONFIG_JOB_ID);
          SalesforceBulkUtil.closeJob(bulkConnection, jobId);
        } catch (AsyncApiException e) {
          throw new RuntimeException(
              String.format("Failed to commit a Salesforce bulk job: %s", e.getMessage()),
              e);
        }
      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return true;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
