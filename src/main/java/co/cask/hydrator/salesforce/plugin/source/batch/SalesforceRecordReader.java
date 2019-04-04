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
import co.cask.hydrator.salesforce.SalesforceConnectionUtil;
import co.cask.hydrator.salesforce.authenticator.Authenticator;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

/**
 * RecordReader implementation, which reads a single Salesforce batch from bulk job
 * provided in InputSplit
 */
public class SalesforceRecordReader extends RecordReader<NullWritable, CSVRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordReader.class);

  private BulkConnection bulkConnection;
  private String jobId;
  private String batchId;
  private BufferedReader queryReader;
  private CSVParser csvParser;
  private Iterator<CSVRecord> parserIterator;

  private CSVRecord value;

  private long linesNumber;
  private long processedLines;

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
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      String queryResponse = SalesforceBulkUtil.waitForBatchResults(bulkConnection, jobId, batchId);

      setupParser(queryResponse);

      linesNumber = countLines(queryResponse);
      processedLines = 1;
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /**
   * Reads single record from csv.
   *
   * @return returns false if no more data to read
   * @throws IOException exception from readLine from query csv
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    if (!parserIterator.hasNext()) {
      return false;
    }

    value = parserIterator.next();
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public CSVRecord getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return processedLines / (float) linesNumber;
  }

  @Override
  public void close() throws IOException {
    if (csvParser != null) {
      csvParser.close();
    }
    if (queryReader != null) {
      queryReader.close();
    }
  }

  private static int countLines(String str) {
    String[] lines = str.split("\r\n|\r|\n");
    return  lines.length;
  }

  @VisibleForTesting
  void setupParser(String queryResponse) throws IOException {
    queryReader = new BufferedReader(new StringReader(queryResponse));

    csvParser = CSVFormat.DEFAULT.
      withHeader().
      withQuoteMode(QuoteMode.ALL).
      withAllowMissingColumnNames(false).
      parse(queryReader);

    if (csvParser.getHeaderMap().isEmpty()) {
      throw new IllegalStateException("Empty response was received from Salesforce, but csv header was expected.");
    }

    parserIterator = csvParser.iterator();
  }
}
