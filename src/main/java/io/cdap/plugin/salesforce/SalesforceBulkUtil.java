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

package io.cdap.plugin.salesforce;

import com.google.common.base.Preconditions;
import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.SessionHeader_element;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class which provides functions to submit jobs to bulk api and read resulting batches
 */
public final class SalesforceBulkUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBulkUtil.class);


  /**
   * Create a new job using the Bulk API.
   *
   * @param bulkConnection  BulkConnection object that will connect to salesforce server using bulk APIs.
   * @param sObject         sObject name
   * @param operationEnum   Operation that need to be performed on sObject
   * @param externalIdField externalIdField will be used in case of update/upsert operation.
   * @param concurrencyMode concurrencyMode can be serial/parallel.
   * @param contentType     contentType will be CSV for query jobs and ZIP_CSV for insert jobs.
   * @return The JobInfo for the new job.
   * @throws AsyncApiException if there is an issue creating the job
   */

  public static JobInfo createJob(BulkConnection bulkConnection, String sObject, OperationEnum operationEnum,
                                  @Nullable String externalIdField,
                                  ConcurrencyMode concurrencyMode, ContentType contentType) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sObject);
    job.setOperation(operationEnum);
    job.setConcurrencyMode(concurrencyMode);
    job.setContentType(contentType);
    if (externalIdField != null) {
      job.setExternalIdFieldName(externalIdField);
    }

    job = bulkConnection.createJob(job);
    Preconditions.checkState(job.getId() != null, "Couldn't get job ID. There was a problem in creating the " +
      "batch job");
    return bulkConnection.getJobStatus(job.getId());
  }

  /**
   * Close a job in Salesforce
   *
   * @param bulkConnection bulk connection instance
   * @param jobId          a job id
   * @throws AsyncApiException if there is an issue creating the job
   */
  public static void closeJob(BulkConnection bulkConnection, String jobId) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    bulkConnection.updateJob(job);
  }

  /**
   * Gets the results of the insert operation for every batch and checks them for errors.
   *
   * @param bulkConnection bulk connection instance
   * @param job            a Salesforce job
   * @param batchInfoList  a list of batches to check
   * @param ignoreFailures if true, unsuccessful row insertions do not cause an exception
   * @throws AsyncApiException if there is an issue checking for batch results
   * @throws IOException       reading csv from Salesforce failed
   */
  public static void checkResults(BulkConnection bulkConnection, JobInfo job,
                                  List<BatchInfo> batchInfoList, boolean ignoreFailures)
    throws AsyncApiException, IOException {

    for (BatchInfo batchInfo : batchInfoList) {
      /*
      The response is a CSV with the following headers:
      Id,Success,Created,Error
       */
      CSVReader rdr = new CSVReader(bulkConnection.getBatchResultStream(job.getId(), batchInfo.getId()));
      List<String> resultHeader = rdr.nextRecord();
      int successRowId = resultHeader.indexOf("Success");
      int errorRowId = resultHeader.indexOf("Error");

      List<String> row;
      while ((row = rdr.nextRecord()) != null) {
        boolean success = Boolean.parseBoolean(row.get(successRowId));
        if (!success) {
          String error = row.get(errorRowId);
          String errorMessage = String.format("Failed to create row with error: '%s'. BatchId='%s'",
                                              error, batchInfo.getId());
          if (ignoreFailures) {
            LOG.error(errorMessage);
          } else {
            throw new BulkAPIBatchException(errorMessage, batchInfo);
          }
        }
      }
    }
  }

  /**
   * Wait for a job to complete by polling the Bulk API.
   *
   * @param bulkConnection BulkConnection used to check results.
   * @param job            The job awaiting completion.
   * @param batchInfoList  List of batches for this job.
   * @param ignoreFailures if true, unsuccessful row insertions do not cause an exception
   */
  public static void awaitCompletion(BulkConnection bulkConnection, JobInfo job,
                                     List<BatchInfo> batchInfoList, boolean ignoreFailures) {
    Set<String> incomplete = batchInfoList
      .stream()
      .map(BatchInfo::getId)
      .collect(Collectors.toSet());

    long batchWaitTimeSeconds = SalesforceSourceConstants.GET_BATCH_WAIT_TIME_SECONDS;
    if (job.getConcurrencyMode().equals(ConcurrencyMode.Serial)) {
      batchWaitTimeSeconds = SalesforceSourceConstants.GET_BATCH_WAIT_TIME_SECONDS_SERIAL_MODE;
    }
    AtomicInteger failures = new AtomicInteger(0);
    Awaitility.await()
      .atMost(batchWaitTimeSeconds, TimeUnit.SECONDS)
      .pollInterval(SalesforceSourceConstants.GET_SINK_BATCH_RESULTS_SLEEP_MS, TimeUnit.MILLISECONDS)
      .until(() -> {
        try {
          BatchInfo[] statusList =
            bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();

          for (BatchInfo b : statusList) {
            if (b.getState() == BatchStateEnum.Failed) {
              if (ignoreFailures) {
                LOG.error(String.format("Batch failed with error: '%s'. BatchId='%s'. This error will be ignored and " +
                                          "pipeline will continue.", b.getStateMessage(), b.getId()));
                incomplete.remove(b.getId());
              } else {
                throw new BulkAPIBatchException("Batch failed", b);
              }
            } else if (b.getState() == BatchStateEnum.Completed) {
              LOG.debug("Batch Completed with Batch Id:{}, Total Processed Records: {}, Failed Records: {}," +
                          " Successful records: {}",
                        b.getId(),
                        b.getNumberRecordsProcessed(),
                        b.getNumberRecordsFailed(),
                        b.getNumberRecordsProcessed() - b.getNumberRecordsFailed());
              incomplete.remove(b.getId());
            }
          }
        } catch (AsyncApiException e) {
          if (AsyncExceptionCode.InvalidSessionId == e.getExceptionCode()) {
            renewSession(bulkConnection, e);
          } else if (AsyncExceptionCode.ClientInputError == e.getExceptionCode() &&
            failures.get() < SalesforceSourceConstants.MAX_RETRIES_ON_API_FAILURE) {
            // This error can occur when server is not responding with proper error message due to network glitch.
            LOG.warn("Error while calling Salesforce APIs. Retrying again");
            failures.getAndIncrement();
          } else {
            throw e;
          }
        }

        return incomplete.isEmpty();
      });
  }

  /**
   * Renew session if bulk connection resets
   *
   * @param connection Bulk Connection
   * @param e          AsyncApiException
   * @throws AsyncApiException
   */
  private static void renewSession(BulkConnection connection, AsyncApiException e) throws AsyncApiException {
    ConnectorConfig config = connection.getConfig();
    try {
      SessionRenewer.SessionRenewalHeader sessionHeader = config.getSessionRenewer().renewSession(config);
      config.setSessionId(((SessionHeader_element) sessionHeader.headerElement).getSessionId());
    } catch (ConnectionException ex) {
      // Can't renew the session - log an error and throw the original AsyncApiException
      LOG.error("Exception renewing session", ex);
      throw e;
    }
  }
}
