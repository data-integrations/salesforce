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

import com.google.common.collect.ImmutableSet;
import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.ws.ConnectorConfig;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * BulkConnectionRetryWrapper class to retry all the salesforce api calls in case of failure.
 */
public class BulkConnectionRetryWrapper {
  public static final Set<AsyncExceptionCode> RETRY_ON_REASON = ImmutableSet.of(AsyncExceptionCode.Unknown,
                                                                                AsyncExceptionCode.InternalServerError,
                                                                                AsyncExceptionCode.ClientInputError,
                                                                                AsyncExceptionCode.Timeout);
  private final BulkConnection bulkConnection;
  private final RetryPolicy<Object> retryPolicy;
  private static final Logger LOG = LoggerFactory.getLogger(BulkConnectionRetryWrapper.class);

  public BulkConnectionRetryWrapper(BulkConnection bulkConnection, boolean retryOnBackendError,
                                    long initialRetryDuration, long maxRetryDuration, int maxRetryCount) {
    this.bulkConnection = bulkConnection;
    this.retryPolicy = SalesforceSplitUtil.getRetryPolicy(initialRetryDuration, maxRetryDuration, maxRetryCount,
                                                          retryOnBackendError);
  }

  public JobInfo createJob(JobInfo jobInfo) throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.createJob(jobInfo), "Failed while creating job.");
  }

  public JobInfo getJobStatus(String jobId) throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.getJobStatus(jobId), "Failed while getting job status.");
  }

  public BatchInfoList getBatchInfoList(String jobId) throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.getBatchInfoList(jobId), "Failed while getting batch info list.");
  }

  public BatchInfo getBatchInfo(String jobId, String batchId) throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.getBatchInfo(jobId, batchId), "Failed while getting batch status.");
  }

  public InputStream getBatchResultStream(String jobId, String batchId) throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.getBatchResultStream(jobId, batchId),
                            "Failed while getting batch result stream.");
  }

  public InputStream getQueryResultStream(String jobId, String batchId, String resultId) throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.getQueryResultStream(jobId, batchId, resultId),
                            "Failed while getting query result stream.");
  }

  public BatchInfo createBatchFromStream(String query, JobInfo job) throws AsyncApiException,
    SalesforceQueryExecutionException, IOException {
    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      return executeWithRetry(() -> bulkConnection.createBatchFromStream(job, bout),
                              String.format("The bulk query job %s failed. Job State: %s.",
                                            job.getId(), job.getState()));
    }
  }

  public ConnectorConfig getConfig() {
    return bulkConnection.getConfig();
  }

  public String[] getQueryResultList(String jobId, String batchId)
    throws AsyncApiException {
    return executeWithRetry(() -> bulkConnection.getQueryResultList(jobId, batchId).getResult(),
                            String.format("The bulk query job %s failed.", jobId));
  }

  private <T> T executeWithRetry(Callable<T> operation, String errorContext) throws AsyncApiException {
    try {
      return Failsafe.with(retryPolicy).get(() -> {
        try {
          T result = operation.call();
          if (result == null) {
            throw new IllegalArgumentException(errorContext);
          }
          return result;
        } catch (AsyncApiException e) {
          if (BulkConnectionRetryWrapper.RETRY_ON_REASON.contains(e.getExceptionCode())) {
            throw new SalesforceQueryExecutionException(e);
          }
          throw e;
        }
      });
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof AsyncApiException) {
        throw (AsyncApiException) ex.getCause();
      }
      throw ex;
    }
  }
}
