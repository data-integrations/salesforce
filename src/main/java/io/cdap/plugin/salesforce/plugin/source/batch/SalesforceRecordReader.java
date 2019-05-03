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
import com.sforce.async.BulkConnection;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
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
public class SalesforceRecordReader extends RecordReader<Schema, Map<String, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordReader.class);

  private final Schema schema;

  private CSVParser csvParser;
  private Iterator<CSVRecord> parserIterator;

  private Map<String, String> value;

  public SalesforceRecordReader(Schema schema) {
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
    String jobId = salesforceSplit.getJobId();
    String batchId = salesforceSplit.getBatchId();
    LOG.debug("Executing Salesforce Batch Id: '{}' for Job Id: '{}'", batchId, jobId);

    Configuration conf = taskAttemptContext.getConfiguration();
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      BulkConnection bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      InputStream queryResponseStream = SalesforceBulkUtil.waitForBatchResults(bulkConnection, jobId, batchId);
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
  public Map<String, String> getCurrentValue() {
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
}
