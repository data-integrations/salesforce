/*
 * Copyright © 2025 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.source.batch.util;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.ws.ConnectorConfig;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBulkRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * BulkConnectionRetryWrapper class to retry all the salesforce api calls in case of failure.
 */
public class BulkConnectionRetryWrapper {

  private final BulkConnection bulkConnection;
  private final RetryPolicy retryPolicy;
  private static final Logger LOG = LoggerFactory.getLogger(BulkConnectionRetryWrapper.class);
  private final boolean retryOnBackendError;
  private final long maxRetryDuration;
  private final int maxRetryCount;
  private final long initialRetryDuration;

  public BulkConnectionRetryWrapper(BulkConnection bulkConnection, boolean isRetryRequired,
                                    long initialRetryDuration, long maxRetryDuration, int maxRetryCount) {
    this.bulkConnection = bulkConnection;
    this.retryOnBackendError = isRetryRequired;
    this.initialRetryDuration = initialRetryDuration;
    this.maxRetryDuration = maxRetryDuration;
    this.maxRetryCount = maxRetryCount;
    this.retryPolicy = SalesforceSplitUtil.getRetryPolicy(initialRetryDuration, maxRetryDuration, maxRetryCount);
  }

  public JobInfo createJob(JobInfo jobInfo) throws AsyncApiException {
    if (!retryOnBackendError) {
      return bulkConnection.createJob(jobInfo);
    }
    Object resultJobInfo = Failsafe.with(retryPolicy).onFailure(event -> LOG.info("Failed while creating job."))
        .get(() -> {
          try {
            return bulkConnection.createJob(jobInfo);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (JobInfo) resultJobInfo;
  }

  public JobInfo getJobStatus(String jobId) throws AsyncApiException {
    if (!retryOnBackendError) {
      return bulkConnection.getJobStatus(jobId);
    }
    Object resultJobInfo = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting job status."))
        .get(() -> {
          try {
            return bulkConnection.getJobStatus(jobId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (JobInfo) resultJobInfo;
  }

  public void updateJob(JobInfo jobInfo) throws AsyncApiException {
    if (!retryOnBackendError) {
      bulkConnection.updateJob(jobInfo);
      return;
    }
    Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while updating job."))
        .get(() -> {
          try {
            return bulkConnection.updateJob(jobInfo);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
  }

  public BatchInfoList getBatchInfoList(String jobId) throws AsyncApiException {
    if (!retryOnBackendError) {
      return bulkConnection.getBatchInfoList(jobId);
    }
    Object batchInfoList = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting batch info list."))
        .get(() -> {
          try {
            return bulkConnection.getBatchInfoList(jobId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (BatchInfoList) batchInfoList;
  }

  public BatchInfo getBatchInfo(String jobId, String batchId) throws AsyncApiException {
    if (!retryOnBackendError) {
      return bulkConnection.getBatchInfo(jobId, batchId);
    }
    Object batchInfo = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting batch status."))
        .get(() -> {
          try {
            return bulkConnection.getBatchInfo(jobId, batchId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (BatchInfo) batchInfo;
  }

  public InputStream getBatchResultStream(String jobId, String batchId) throws AsyncApiException {
    if (!retryOnBackendError) {
      return bulkConnection.getBatchResultStream(jobId, batchId);
    }
    Object inputStream = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting batch result stream."))
        .get(() -> {
          try {
            return bulkConnection.getBatchResultStream(jobId, batchId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (InputStream) inputStream;
  }

  public InputStream getQueryResultStream(String jobId, String batchId, String resultId) throws AsyncApiException {
    if (!retryOnBackendError) {
      return bulkConnection.getQueryResultStream(jobId, batchId, resultId);
    }
    Object inputStream = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting query result stream."))
        .get(() -> {
          try {
            return bulkConnection.getQueryResultStream(jobId, batchId, resultId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (InputStream) inputStream;
  }

  public BatchInfo createBatchFromStream(String query, JobInfo job) throws AsyncApiException,
    SalesforceQueryExecutionException, IOException {
    if (!retryOnBackendError) {
      return createBatchFromStreamI(query, job);
    }
    Object batchInfo = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while creating batch from stream."))
        .get(() -> {
          try {
            return createBatchFromStreamI(query, job);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e);
          }
        });
    return (BatchInfo) batchInfo;
  }

  private BatchInfo createBatchFromStreamI(String query, JobInfo job) throws
    SalesforceQueryExecutionException, IOException, AsyncApiException {
    BatchInfo batchInfo = null;
    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      batchInfo = bulkConnection.createBatchFromStream(job, bout);
    } catch (AsyncApiException exception) {
      LOG.warn("The bulk query job {} failed. Job State: {}.", job.getId(), job.getState());
      if (SalesforceBulkRecordReader.RETRY_ON_REASON.contains(exception.getExceptionCode())) {
        throw new SalesforceQueryExecutionException(exception);
      }
      throw exception;
    }
    return batchInfo;
  }

  public ConnectorConfig getConfig() {
    return bulkConnection.getConfig();
  }
  
  public BulkConnection getBukConnection() {
    return bulkConnection;
  }
}
