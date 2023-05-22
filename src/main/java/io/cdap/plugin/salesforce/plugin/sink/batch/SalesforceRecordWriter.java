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
import com.sforce.async.BatchInfo;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes csv records into batches and submits them to Salesforce Bulk job.
 * Accepts <code>null</code> as a key, and CSVRecord as a value.
 */
public class SalesforceRecordWriter extends RecordWriter<NullWritable, CSVRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordWriter.class);

  private BulkConnection bulkConnection;
  private JobInfo jobInfo;
  private ErrorHandling errorHandling;
  private Long maxBytesPerBatch;
  private Long maxRecordsPerBatch;
  private List<BatchInfo> batchInfoList = new ArrayList<>();
  private CSVBuffer csvBuffer;
  private CSVBuffer csvBufferSizeCheck;

  public SalesforceRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, AsyncApiException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String jobId = conf.get(SalesforceSinkConstants.CONFIG_JOB_ID);

    // these are already validated no need to validate again
    errorHandling = ErrorHandling.fromValue(conf.get(SalesforceSinkConstants.CONFIG_ERROR_HANDLING)).get();
    maxBytesPerBatch = Long.parseLong(conf.get(SalesforceSinkConstants.CONFIG_MAX_BYTES_PER_BATCH));
    maxRecordsPerBatch = Long.parseLong(conf.get(SalesforceSinkConstants.CONFIG_MAX_RECORDS_PER_BATCH));

    csvBuffer = new CSVBuffer(true);
    csvBufferSizeCheck = new CSVBuffer(false);

    AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
    bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
    jobInfo = bulkConnection.getJobStatus(jobId);
  }

  @Override
  public void write(NullWritable key, CSVRecord csvRecord) throws IOException {
    csvBufferSizeCheck.reset();
    csvBufferSizeCheck.write(csvRecord);

    if (csvBuffer.size() + csvBufferSizeCheck.size() > maxBytesPerBatch ||
      csvBuffer.getRecordsCount() >= maxRecordsPerBatch) {
      submitCurrentBatch();
    }

    csvBuffer.write(csvRecord);
  }

  private void submitCurrentBatch() throws IOException {
    if (csvBuffer.getRecordsCount() != 0) {
      InputStream csvInputStream = new ByteArrayInputStream(csvBuffer.getByteArray());
      try {
        BatchInfo batchInfo = bulkConnection.createBatchFromStream(jobInfo, csvInputStream);
        batchInfoList.add(batchInfo);
        LOG.info("Submitted a batch with batchId='{}'", batchInfo.getId());
      } catch (AsyncApiException e) {
        throw new RuntimeException(
          String.format("Failed to create and submit a batch for writes: %s", e.getMessage()),
          e);
      }
      csvBuffer.reset();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    submitCurrentBatch();

    try {
      SalesforceBulkUtil.awaitCompletion(bulkConnection, jobInfo, batchInfoList,
                                         errorHandling.equals(ErrorHandling.SKIP));
      SalesforceBulkUtil.checkResults(bulkConnection, jobInfo, batchInfoList, errorHandling.equals(ErrorHandling.SKIP));
    } catch (AsyncApiException | ConditionTimeoutException | BulkAPIBatchException e) {
      throw new RuntimeException(String.format("Failed to check the result of a batch for writes: %s",
                                               e.getMessage()), e);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Pipeline Failed due to error: %s", e.getMessage()), e);
    } finally {
      try {
        csvBufferSizeCheck.close();
      } catch (IOException ex) {
        throw ex;
      } finally {
        csvBuffer.close();
      }
    }
  }
}
