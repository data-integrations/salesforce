/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class which provides methods to generate Salesforce splits for a query.
 */
public final class SalesforceSplitUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSplitUtil.class);

  /**
   * Generates and returns Salesforce splits for a query
   * @param query the query for the sobject
   * @param bulkConnection used to create salesforce jobs
   * @param enablePKChunk indicates if pk chunking is enabled
   * @param operation indicates they query type
   * @return list of salesforce splits
   */
  public static List<SalesforceSplit> getQuerySplits(String query, BulkConnection bulkConnection, boolean enablePKChunk, String operation) {
    return Stream.of(getBatches(query, bulkConnection, enablePKChunk, operation))
      .map(batch -> new SalesforceSplit(batch.getJobId(), batch.getId(), query))
      .collect(Collectors.toList());
  }

  /**
   * Based on query length sends query to Salesforce to receive array of batch info. If query is within limit, executes
   * original query. If not, switches to wide object logic, i.e. generates Id query to retrieve batch info for Ids only
   * that will be used later to retrieve data using SOAP API.
   *
   * @param query SOQL query
   * @param bulkConnection bulk connection
   * @param enablePKChunk enable PK Chunking
   * @param operation indicates they query type
   * @return array of batch info
   */
  private static BatchInfo[] getBatches(
          String query, BulkConnection bulkConnection, boolean enablePKChunk, String operation) {
    try {
      if (!SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
        LOG.debug("Wide object query detected. Query length '{}'", query.length());
        query = SalesforceQueryUtil.createSObjectIdQuery(query);
      }
      BatchInfo[] batches = runBulkQuery(bulkConnection, query, enablePKChunk, operation);
      LOG.debug("Number of batches received from Salesforce: '{}'", batches.length);
      return batches;
    } catch (AsyncApiException | IOException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /**
   * Start batch job of reading a given guery result.
   *
   * @param bulkConnection bulk connection instance
   * @param query a SOQL query
   * @param enablePKChunk enable PK Chunk
   * @param operation to be preformed (query or queryAll)
   * @return an array of batches
   * @throws AsyncApiException  if there is an issue creating the job
   * @throws IOException failed to close the query
   */
  private static BatchInfo[] runBulkQuery(BulkConnection bulkConnection, String query, boolean enablePKChunk, String operation)
    throws AsyncApiException, IOException {

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    JobInfo job = SalesforceBulkUtil.createJob(bulkConnection, sObjectDescriptor.getName(), OperationEnum.valueOf(operation), null);
    BatchInfo batchInfo;
    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      batchInfo = bulkConnection.createBatchFromStream(job, bout);
    }

    if (enablePKChunk) {
      LOG.debug("PKChunking is enabled");
      return waitForBatchChunks(bulkConnection, job.getId(), batchInfo.getId());
    }
    LOG.debug("PKChunking is not enabled");
    BatchInfo[] batchInfos = bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();
    LOG.info("Job id {}, status: {}", job.getId(), bulkConnection.getJobStatus(job.getId()).getState());
    if (batchInfos.length > 0) {
      LOG.info("Batch size {}, state {}", batchInfos.length, batchInfos[0].getState());
    }
    return batchInfos;
  }


  /**
   * Initializes bulk connection based on given Hadoop credentials configuration.
   *
   * @return bulk connection instance
   */
  public static BulkConnection getBulkConnection(String username, String password, String consumerKey, String consumerSecret, String loginUrl) {
    AuthenticatorCredentials authenticatorCredentials = SalesforceConnectionUtil
      .getAuthenticatorCredentials(username, password, consumerKey, consumerSecret, loginUrl);
    try {
      return new BulkConnection(Authenticator.createConnectorConfig(authenticatorCredentials));
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /**
   * Initializes bulk connection based on given Hadoop credentials configuration.
   *
   * @return bulk connection instance
   */
  public static BulkConnection getBulkConnection(AuthenticatorCredentials authenticatorCredentials) {
    try {
      return new BulkConnection(Authenticator.createConnectorConfig(authenticatorCredentials));
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /** When PK Chunk is enabled, wait for state of initial batch to be NotProcessed, in this case Salesforce API will
   * decide how many batches will be created
   * @param bulkConnection bulk connection instance
   * @param jobId a job id
   * @param initialBatchId a batch id
   * @return Array with Batches created by Salesforce API
   *
   * @throws AsyncApiException if there is an issue creating the job
   */
  private static BatchInfo[] waitForBatchChunks(BulkConnection bulkConnection, String jobId, String initialBatchId)
    throws AsyncApiException {
    BatchInfo initialBatchInfo = null;
    for (int i = 0; i < SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES; i++) {
      //check if the job is aborted
      if (bulkConnection.getJobStatus(jobId).getState() == JobStateEnum.Aborted) {
        LOG.info(String.format("Job with Id: '%s' is aborted", jobId));
        return new BatchInfo[0];
      }
      try {
        initialBatchInfo = bulkConnection.getBatchInfo(jobId, initialBatchId);
      } catch (AsyncApiException e) {
        if (i == SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES - 1) {
          throw e;
        }
        LOG.warn("Failed to get info for batch {}. Will retry after some time.", initialBatchId, e);
        continue;
      }

      if (initialBatchInfo.getState() == BatchStateEnum.NotProcessed) {
        BatchInfo[] result = bulkConnection.getBatchInfoList(jobId).getBatchInfo();
        return Arrays.stream(result).filter(batchInfo -> batchInfo.getState() != BatchStateEnum.NotProcessed)
          .toArray(BatchInfo[]::new);
      } else if (initialBatchInfo.getState() == BatchStateEnum.Failed) {
        throw new BulkAPIBatchException("Batch failed", initialBatchInfo);
      } else {
        try {
          Thread.sleep(SalesforceSourceConstants.GET_BATCH_RESULTS_SLEEP_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Job is aborted", e);
        }
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", initialBatchInfo);
  }

  public static void closeJobs(List<String> jobIds, AuthenticatorCredentials authenticatorCredentials) {
    BulkConnection bulkConnection = SalesforceSplitUtil.getBulkConnection(authenticatorCredentials);
    RuntimeException runtimeException = null;
    for (String jobId : jobIds) {
      try {
        SalesforceBulkUtil.closeJob(bulkConnection, jobId);
      } catch (AsyncApiException e) {
        if (runtimeException == null) {
          runtimeException = new RuntimeException(e);
        } else {
          runtimeException.addSuppressed(e);
        }
      }
    }
    if (runtimeException != null) {
      throw runtimeException;
    }
  }
}
