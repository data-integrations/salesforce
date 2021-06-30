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

import com.google.common.annotations.VisibleForTesting;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.QueryResultList;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * RecordReader implementation, which reads a single Salesforce batch from bulk job
 * provided in InputSplit
 */
public class SalesforceBulkRecordReader extends RecordReader<Schema, Map<String, ?>> {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBulkRecordReader.class);

  private final Schema schema;

  private CSVParser csvParser;
  private Iterator<CSVRecord> parserIterator;
  private Map<String, ?> value;
  private String jobId;
  private BulkConnection bulkConnection;

  public SalesforceBulkRecordReader(Schema schema) {
    this.schema = schema;
  }

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
    String batchId = salesforceSplit.getBatchId();
    LOG.debug("Executing Salesforce Batch Id: '{}' for Job Id: '{}'", batchId, jobId);

    Configuration conf = taskAttemptContext.getConfiguration();
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      InputStream queryResponseStream = waitForBatchResults(bulkConnection, jobId, batchId);
      setupParser(queryResponseStream);
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /**
   * Reads single record from csv.
   *
   * @return returns false if no more data to read
   */
  @Override
  public boolean nextKeyValue() {
    if (!parserIterator.hasNext()) {
      return false;
    }

    value = parserIterator.next().toMap();
    return true;
  }

  @Override
  public Schema getCurrentKey() {
    return schema;
  }

  @Override
  public Map<String, ?> getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
    if (csvParser != null) {
      // this also closes the inputStream
      csvParser.close();
    }
    try {
      SalesforceBulkUtil.closeJob(bulkConnection, jobId);
    } catch (AsyncApiException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void setupParser(InputStream queryResponseStream) throws IOException {
    CSVFormat csvFormat = CSVFormat.DEFAULT.
      withHeader().
      withQuoteMode(QuoteMode.ALL).
      withAllowMissingColumnNames(false);

    csvParser = CSVParser.parse(queryResponseStream, StandardCharsets.UTF_8, csvFormat);

    if (csvParser.getHeaderMap().isEmpty()) {
      throw new IllegalStateException("Empty response was received from Salesforce, but csv header was expected.");
    }

    parserIterator = csvParser.iterator();
  }

  /**
   * Wait until a batch with given batchId succeeds, or throw an exception
   *
   * @param bulkConnection bulk connection instance
   * @param jobId a job id
   * @param batchId a batch id
   * @return an input stream which represents a current batch response, which is a bunch of lines in csv format.
   *
   * @throws AsyncApiException  if there is an issue creating the job
   * @throws InterruptedException sleep interrupted
   */
  public InputStream waitForBatchResults(BulkConnection bulkConnection, String jobId, String batchId)
    throws AsyncApiException, InterruptedException {

    BatchInfo info = null;
    for (int i = 0; i < SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES; i++) {
      info = bulkConnection.getBatchInfo(jobId, batchId);

      if (info.getState() == BatchStateEnum.Completed) {
        QueryResultList list =
          bulkConnection.getQueryResultList(jobId, batchId);
        String[] resultIds = list.getResult();

        List<InputStream> streams = new ArrayList<>(resultIds.length);
        for (String resultId : resultIds) {
          streams.add(bulkConnection.getQueryResultStream(jobId, batchId, resultId));
        }

        return new SequenceInputStream(Collections.enumeration(streams));
      } else if (info.getState() == BatchStateEnum.Failed) {

        throw new BulkAPIBatchException("Batch failed", info);
      } else {
        Thread.sleep(SalesforceSourceConstants.GET_BATCH_RESULTS_SLEEP_MS);
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", info);
  }
}
