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
import io.cdap.cdap.api.data.format.StructuredRecord;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes csv records into batches and submits them to Salesforce Bulk job.
 * Accepts <code>null</code> as a key, and StructuredRecord as a value.
 */
public class SalesforceRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordWriter.class);

  private BulkConnection bulkConnection;
  private JobInfo jobInfo;
  private ErrorHandling errorHandling;
  private Long maxBytesPerBatch;
  private Long maxRecordsPerBatch;
  private List<BatchInfo> batchInfoList = new ArrayList<>();
  private CSVBuffer csvBuffer;
  private CSVBuffer csvBufferSizeCheck;
  private final StructuredRecordToCSVRecordTransformer transformer = new StructuredRecordToCSVRecordTransformer();
  private int count = 0;
  private int filesArraySize = 0;
  private final Map<String, InputStream> attachmentMap = new HashMap<>();
  private final boolean isFileUploadObject;
  private FileUploadSobject fileUploadObject;
  Base64.Decoder base64Decoder = Base64.getDecoder();

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
    isFileUploadObject = FileUploadSobject.isFileUploadSobject(jobInfo.getObject());
    if (isFileUploadObject) {
      fileUploadObject = FileUploadSobject.valueOf(jobInfo.getObject());
    }
  }

  @Override
  public void write(NullWritable key, StructuredRecord record) throws IOException {
    CSVRecord csvRecord = transformer.transform(record, fileUploadObject, ++count);
    csvBufferSizeCheck.reset();
    csvBufferSizeCheck.write(csvRecord);

    if (csvBuffer.size() + csvBufferSizeCheck.size() + filesArraySize + getFileUploadSize(record) > maxBytesPerBatch ||
      csvBuffer.getRecordsCount() >= maxRecordsPerBatch) {
      submitCurrentBatch();
    }

    if (isFileUploadObject) {
      populateInputStreamMap(record);
    }
    csvBuffer.write(csvRecord);
  }

  /**
   * This method will return the attachment file size in bytes. Exception will be thrown if file size exceeds max
   * batch size. Max batch size can not be more than 10 MB.
   *
   * @param record StructuredRecord to get the data file from it.
   * @return fileSizeInBytes
   */
  private int getFileUploadSize(StructuredRecord record) {
    if (isFileUploadObject) {
      String encodedFileData = record.get(fileUploadObject.getDataField());
      int fileSizeInBytes = base64Decoder.decode(encodedFileData).length;
      if (fileSizeInBytes > maxBytesPerBatch) {
        throw new RuntimeException("File Size exceeded the limit of maximum bytes per batch");
      }
      return fileSizeInBytes;
    }
    return 0;
  }

  /**
   * This method will populate the attachment map with file name as key and value as InputStream for the file.
   *
   * @param record StructuredRecord to get the data file from it to put into attachment map.
   */
  private void populateInputStreamMap(StructuredRecord record) {
    String encodedFileData = record.get(fileUploadObject.getDataField());
    byte[] byteArray = base64Decoder.decode(encodedFileData);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
    filesArraySize = filesArraySize + byteArray.length;
    attachmentMap.put(transformer.getAttachmentKey(count, record.get(fileUploadObject.getNameField())),
                      byteArrayInputStream);
  }

  private void submitCurrentBatch() throws IOException {
    if (csvBuffer.getRecordsCount() != 0) {
      InputStream csvInputStream = new ByteArrayInputStream(csvBuffer.getByteArray());
      try {
        BatchInfo batchInfo = bulkConnection.createBatchWithInputStreamAttachments(jobInfo, csvInputStream,
                                                                                   attachmentMap);
        batchInfoList.add(batchInfo);
        LOG.info("Submitted a batch with batchId='{}'", batchInfo.getId());
      } catch (AsyncApiException e) {
        throw new RuntimeException(
          String.format("Failed to create and submit a batch for writes: %s", e.getMessage()),
          e);
      }
      attachmentMap.clear();
      filesArraySize = 0;
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
