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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
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
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
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
  private String batchId;
  private String[] resultIds;
  private int resultIdIndex;

  public SalesforceBulkRecordReader(Schema schema) {
    this(schema, null, null, null);
  }

  @VisibleForTesting
  SalesforceBulkRecordReader(Schema schema, String jobId, String batchId, String[] resultIds) {
    this.schema = schema;
    this.resultIdIndex = 0;
    this.jobId = jobId;
    this.batchId = batchId;
    this.resultIds = resultIds;
  }

  /**
   * Get csv from a single Salesforce batch
   *
   * @param inputSplit         specifies batch details
   * @param taskAttemptContext task context
   * @throws IOException          can be due error during reading query
   * @throws InterruptedException interrupted sleep while waiting for batch results
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {

    SalesforceSplit salesforceSplit = (SalesforceSplit) inputSplit;
    jobId = salesforceSplit.getJobId();
    batchId = salesforceSplit.getBatchId();
    LOG.debug("Executing Salesforce Batch Id: '{}' for Job Id: '{}'", batchId, jobId);

    Configuration conf = taskAttemptContext.getConfiguration();
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      resultIds = waitForBatchResults(bulkConnection);
      LOG.debug("Batch {} returned {} results", batchId, resultIds.length);
      setupParser();
    } catch (AsyncApiException e) {
      throw new RuntimeException(
        String.format("Failed to wait for the result of a batch: %s", e.getMessage()),
        e);
    }
  }

  public SalesforceBulkRecordReader initialize(
      InputSplit inputSplit,
      AuthenticatorCredentials credentials)
    throws IOException, InterruptedException {
    SalesforceSplit salesforceSplit = (SalesforceSplit) inputSplit;
    jobId = salesforceSplit.getJobId();
    batchId = salesforceSplit.getBatchId();
    LOG.debug("Executing Salesforce Batch Id: '{}' for Job Id: '{}'", batchId, jobId);

    try {
      bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      resultIds = waitForBatchResults(bulkConnection);
      LOG.debug("Batch {} returned {} results", batchId, resultIds.length);
      setupParser();
    } catch (AsyncApiException e) {
      throw new RuntimeException(
        String.format("Failed to wait for the result of a batch: %s", e.getMessage()),
        e);
    }
    return this;
  }

  /**
   * Reads single record from csv.
   *
   * @return returns false if no more data to read
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    if (parserIterator == null) {
      return false;
    }
    while (!parserIterator.hasNext()) {
      if (resultIdIndex == resultIds.length) {
        // No more result ids to process.
        return false;
      }
      // Close CSV parser for previous result.
      if (csvParser != null && !csvParser.isClosed()) {
        // this also closes the inputStream
        csvParser.close();
        csvParser = null;
      }
      try {
        // Parse the next result.
        setupParser();
      } catch (AsyncApiException e) {
        throw new IOException("Failed to query results", e);
      }
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
    if (csvParser != null && !csvParser.isClosed()) {
      // this also closes the inputStream
      csvParser.close();
      csvParser = null;
    }
  }

  @VisibleForTesting
  void setupParser() throws IOException, AsyncApiException {
    if (resultIdIndex >= resultIds.length) {
      throw new IllegalArgumentException(String.format("Invalid resultIdIndex %d, should be less than %d",
                                                       resultIdIndex, resultIds.length));
    }
    InputStream queryResponseStream = bulkConnection.getQueryResultStream(jobId, batchId, resultIds[resultIdIndex]);

    CSVFormat csvFormat = CSVFormat.DEFAULT
      .withHeader()
      .withQuoteMode(QuoteMode.ALL)
      .withAllowMissingColumnNames(false);
    csvParser = CSVParser.parse(queryResponseStream, StandardCharsets.UTF_8, csvFormat);
    if (csvParser.getHeaderMap().isEmpty()) {
      throw new IllegalStateException("Empty response was received from Salesforce, but csv header was expected.");
    }
    parserIterator = csvParser.iterator();
    resultIdIndex++;
  }

  /**
   * Wait until a batch with given batchId succeeds, or throw an exception
   *
   * @param bulkConnection bulk connection instance
   * @return array of batch result ids
   * @throws AsyncApiException    if there is an issue creating the job
   * @throws InterruptedException sleep interrupted
   */
  private String[] waitForBatchResults(BulkConnection bulkConnection)
    throws AsyncApiException, InterruptedException {

    BatchInfo info = null;
    for (int i = 0; i < SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES; i++) {
      try {
        info = bulkConnection.getBatchInfo(jobId, batchId);
      } catch (AsyncApiException e) {
        if (i == SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES - 1) {
          throw e;
        }
        LOG.warn("Failed to get info for batch {}. Will retry after some time.", batchId, e);
        continue;
      }

      if (info.getState() == BatchStateEnum.Completed) {
        return bulkConnection.getQueryResultList(jobId, batchId).getResult();
      } else if (info.getState() == BatchStateEnum.Failed) {
        throw new BulkAPIBatchException("Batch failed", info);
      } else {
        LOG.debug("Batch {} job {} state {}", batchId, jobId, info.getState());
        Thread.sleep(SalesforceSourceConstants.GET_BATCH_RESULTS_SLEEP_MS);
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", info);
  }
}
