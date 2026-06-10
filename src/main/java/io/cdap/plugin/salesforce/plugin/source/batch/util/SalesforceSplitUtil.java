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

import com.google.common.base.Strings;
import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.TimeoutExceededException;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.parser.SalesforceQueryParser;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Utility class which provides methods to generate Salesforce splits for a query.
 */
public final class SalesforceSplitUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSplitUtil.class);

  /**
   * Generates and returns Salesforce splits for a query
   *
   * @param query          the query for the sobject
   * @param bulkConnection used to create salesforce jobs
   * @param enablePKChunk  indicates if pk chunking is enabled
   * @return list of salesforce splits
   */
  public static List<SalesforceSplit> getQuerySplits(String query, BulkConnectionRetryWrapper bulkConnection,
                                                     boolean enablePKChunk, String operation) {
    return Stream.of(getBatches(query, bulkConnection, enablePKChunk, operation))
      .map(batch -> new SalesforceSplit(batch.getJobId(), batch.getId(), query))
      .collect(Collectors.toList());
  }

  /**
   * Based on query length sends query to Salesforce to receive array of batch info. If query is within limit, executes
   * original query. If not, switches to wide object logic, i.e. generates Id query to retrieve batch info for Ids only
   * that will be used later to retrieve data using SOAP API.
   *
   * @param query          SOQL query
   * @param bulkConnection bulk connection
   * @param enablePKChunk  enable PK Chunking
   * @return array of batch info
   */
  private static BatchInfo[] getBatches(String query, BulkConnectionRetryWrapper bulkConnection,
                                        boolean enablePKChunk, String operation) {
    try {
      if (!SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
        LOG.debug("Wide object query detected. Query length '{}'", query.length());
        query = SalesforceQueryUtil.createSObjectIdQuery(query);
      }
      BatchInfo[] batches = runBulkQuery(bulkConnection, query, enablePKChunk, operation);
      LOG.debug("Number of batches received from Salesforce: '{}'", batches.length);
      return batches;
    } catch (AsyncApiException | IOException | InterruptedException e) {
      throw new RuntimeException(
        String.format("Failed to run a Salesforce bulk query (%s): %s", query, e.getMessage()), e);
    }
  }

  /**
   * Start batch job of reading a given guery result.
   *
   * @param bulkConnection bulk connection instance
   * @param query          a SOQL query
   * @param enablePKChunk  enable PK Chunk
   * @return an array of batches
   * @throws AsyncApiException if there is an issue creating the job
   * @throws IOException       failed to close the query
   */
  private static BatchInfo[] runBulkQuery(BulkConnectionRetryWrapper bulkConnection, String query,
                                          boolean enablePKChunk, String operation)
    throws AsyncApiException, IOException, InterruptedException {

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    JobInfo job = SalesforceBulkUtil.createJob(bulkConnection, sObjectDescriptor.getName(), getOperationEnum(operation),
                                               null, ConcurrencyMode.Parallel, ContentType.CSV);
    BatchInfo batchInfo;
    try {
      batchInfo = bulkConnection.createBatchFromStream(query, job);
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
    } catch (TimeoutExceededException e) {
      throw new AsyncApiException("Exhausted retries trying to create batch from stream", AsyncExceptionCode.Timeout);
    } catch (FailsafeException e) {
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      if (e.getCause() instanceof AsyncApiException) {
        throw (AsyncApiException) e.getCause();
      }
      throw e;
    } catch (SalesforceQueryExecutionException e) {
      throw new RuntimeException(e);
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
      throw new RuntimeException(
        String.format("Failed to create a connection to Salesforce bulk API: %s", e.getMessage()),
        e);
    }
  }

  /**
   * When PK Chunk is enabled, wait for state of initial batch to be NotProcessed, in this case Salesforce API will
   * decide how many batches will be created
   *
   * @param bulkConnection bulk connection instance
   * @param jobId          a job id
   * @param initialBatchId a batch id
   * @return Array with Batches created by Salesforce API
   * @throws AsyncApiException if there is an issue creating the job
   */
  private static BatchInfo[] waitForBatchChunks(BulkConnectionRetryWrapper bulkConnection,
                                                String jobId, String initialBatchId)
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
          throw new RuntimeException(String.format("Job is aborted: %s", e.getMessage()), e);
        }
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", initialBatchInfo);
  }

  public static void closeJobs(Set<String> jobIds, AuthenticatorCredentials authenticatorCredentials) {
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

  private static OperationEnum getOperationEnum(String operation) {
    try {
      return OperationEnum.valueOf(operation);
    } catch (IllegalArgumentException ex) {
      throw new InvalidConfigException("Unsupported value for operation: " + operation,
                                       SalesforceSourceConstants.PROPERTY_OPERATION);
    }
  }

  public static RetryPolicy<Object> getRetryPolicy(Long initialRetryDuration, Long maxRetryDuration,
                                                   Integer maxRetryCount, Boolean retryOnBackendError) {
    // Exponential backoff with initial retry of 5 seconds and max retry of 80 seconds.
    if (retryOnBackendError) {
      return RetryPolicy.builder()
        .handle(SalesforceQueryExecutionException.class)
        .withBackoff(Duration.ofSeconds(initialRetryDuration), Duration.ofSeconds(maxRetryDuration), 2)
        .withMaxRetries(maxRetryCount)
        .onRetry(event -> {
          Throwable t = event.getLastException();
          LOG.warn("Attempt #{} failed while executing job with error: {}", event.getAttemptCount(), t.getMessage(), t);
          LOG.debug("Retrying Salesforce Bulk Query. Retry count: {}", event.getAttemptCount());
        })
        .onRetriesExceeded(event -> LOG.error("Retry limit reached for Salesforce Bulk Query."))
        .build();
    } else {
      return RetryPolicy.builder().withMaxRetries(0).build();
    }
  }

  // This is added for UCS use case only to identify the objects where PK chunking needs to be enabled by default.
  public static boolean isPkChunkingSupported(String sobjectName) {
    if (!Strings.isNullOrEmpty(sobjectName)) {
      return SalesforceSourceConstants.SUPPORTED_OBJECTS_WITH_PK_CHUNK.contains(sobjectName.toLowerCase())
        || isCustomObject(sobjectName);
    }
    return false;
  }

  /**
   * Determines whether PK chunking should be enabled or disabled based on the
   * estimated record count, object support, and query compatibility.
   *
   * @param query       the SOQL query
   * @param credentials authenticator credentials for SOAP API
   * @param threshold   the record count threshold for enabling PK chunking
   * @return true if PK chunking should be enabled, false otherwise
   */
  public static boolean hasRequiredCountForPkChunking(
      final String query,
      final AuthenticatorCredentials credentials,
      final long threshold) {
    try {
      String sObjectName = SObjectDescriptor.fromQuery(query).getName();
      String countQuery = SalesforceQueryUtil.createCountQuery(query);

      try {
        SalesforceQueryUtil.QueryPlanResponse planResponse =
            SalesforceQueryUtil.getQueryPlan(countQuery, credentials);
        if (planResponse != null && planResponse.getPlans() != null
            && !planResponse.getPlans().isEmpty()) {
          SalesforceQueryUtil.QueryPlan leadingPlan =
              planResponse.getPlans().get(0);
          LOG.debug(
              "PK Chunking Query Plan: leading operation is '{}', "
                  + "cardinality is {}, cost is {}",
              leadingPlan.getLeadingOperationType(),
              leadingPlan.getCardinality(),
              leadingPlan.getRelativeCost());
          if (leadingPlan.getCardinality() >= threshold
              || leadingPlan.getRelativeCost() >= 1.0) {
            LOG.info(
                "PK Chunking: Query Plan indicates high volume or "
                    + "non-selective scan. Auto-enabling PK Chunking.");
            return true;
          }
        }
      } catch (Exception e) {
        LOG.warn(
            "PK Chunking: Query Plan check failed, defaulting to true.",
            e);
        return true;
      }

      PartnerConnection partnerConnection =
          SalesforceConnectionUtil.getPartnerConnection(credentials);
      QueryResult result = partnerConnection.query(countQuery);
      int recordCount = result.getSize();
      LOG.debug(
          "PK Chunking validation: object '{}' has {} records, "
              + "threshold is {}",
          sObjectName, recordCount, threshold);
      return recordCount >= threshold;
    } catch (Exception e) {
      LOG.warn(
          "PK Chunking validation: unexpected error during COUNT() query "
              + "check, falling back to default PK chunking",
          e);
      return true;
    }
  }

  /**
   * Helper method to check if sobject is custom object.
   *
   * @param sobjectName name of the sobject
   * @return true if custom, false otherwise
   */
  private static boolean isCustomObject(final String sobjectName) {
    return sobjectName.toLowerCase().endsWith("__c");
  }
}
