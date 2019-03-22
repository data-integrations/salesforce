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

import co.cask.hydrator.salesforce.SalesforceBulkUtil;
import co.cask.hydrator.salesforce.authenticator.Authenticator;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

/**
 * RecordReader implementation, which reads a single salesforce batch from bulk job
 * provided in InputSplit
 */
public class SalesforceRecordReader extends RecordReader<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordReader.class);

  private BulkConnection bulkConnection;
  private String jobId;
  private String batchId;
  private BufferedReader queryReader = null;

  private String key;
  private String value;

  /**
   * Get csv from a single Salesforce batch
   *
   * @param inputSplit specifies batch details
   * @param taskAttemptContext task context
   * @throws IOException can be due error during reading query
   * @throws InterruptedException interrupted sleep while waiting for batch results
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {

    SalesforceSplit salesforceSplit = (SalesforceSplit) inputSplit;
    jobId = salesforceSplit.getJobId();
    batchId = salesforceSplit.getBatchId();

    Configuration conf = taskAttemptContext.getConfiguration();
    SalesforceBatchSource.Config pluginConfig = new SalesforceBatchSource.Config(conf);

    try {
      AuthenticatorCredentials credentials = pluginConfig.getAuthenticatorCredentials();
      bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      String queryResponse = SalesforceBulkUtil.waitForBatchResults(bulkConnection, jobId, batchId);

      queryReader = new BufferedReader(new StringReader(queryResponse));
      key = queryReader.readLine(); // first line of csv contains names of columns
    } catch (AsyncApiException e) {
      throw new RuntimeException("Exception while communicating with Salesforce API", e);
    }
  }

  /**
   * Reads one row from csv. Sometimes the entries in csv can be multiline.
   * That's why one row does not equal one text line.
   *
   * @return returns false if no more data to read
   * @throws IOException exception from readLine from query csv
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    StringBuilder result = new StringBuilder();

    for (;;) {
      String line = queryReader.readLine();

      // end of response for this record reader
      if (line == null) {
        return false;
      }

      result.append(line);

      // All values in Salesforce csv are always enquoted with double-quote. By looking for quote at the end of row
      // we know that the line break is end
      // of csv row and not just line break in value itself.
      if (line.endsWith("\"")) {
        break;
      } else {
        result.append("\n");
      }
    }

    value = result.toString();
    return true;
  }

  @Override
  public String getCurrentKey() {
    return key;
  }

  @Override
  public String getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (queryReader != null) {
      queryReader.close();
    }
  }

  // for testing purposes only
  @VisibleForTesting
  void setQueryReader(BufferedReader queryReader) {
    this.queryReader = queryReader;
  }
}
