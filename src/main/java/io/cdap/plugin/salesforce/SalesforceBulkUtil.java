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
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class which provides functions to submit jobs to bulk api and read resulting batches
 */
public final class SalesforceBulkUtil {

  /**
   * Salesforce Bulk API has a limitation, which is 10 minutes per processing of a batch
   */
  private static final long GET_BATCH_WAIT_TIME_SECONDS = 600;
  /**
   * Sleep time between polling the batch status
   */
  private static final long GET_BATCH_RESULTS_SLEEP_MS = 500;
  /**
   * Number of tries while polling the batch status
   */
  private static final long GET_BATCH_RESULTS_TRIES = GET_BATCH_WAIT_TIME_SECONDS * (1000 / GET_BATCH_RESULTS_SLEEP_MS);


  /**
   * Create a new job using the Bulk API.
   *
   * @return The JobInfo for the new job.
   * @throws AsyncApiException if there is an issue creating the job
   */
  public static JobInfo createJob(BulkConnection bulkConnection, String sObject) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sObject);
    job.setOperation(OperationEnum.query);
    job.setConcurrencyMode(ConcurrencyMode.Parallel);
    job.setContentType(ContentType.CSV);
    job = bulkConnection.createJob(job);
    Preconditions.checkState(job.getId() != null, "Couldn't get job ID. There was a problem in creating the " +
      "batch job");
    return bulkConnection.getJobStatus(job.getId());
  }

  /**
   * Start batch job of reading a given guery result.
   *
   * @param bulkConnection bulk connection instance
   * @param query a SOQL query
   * @return an array of batches
   * @throws AsyncApiException  if there is an issue creating the job
   * @throws IOException failed to close the query
   */
  public static BatchInfo[] runBulkQuery(BulkConnection bulkConnection, String query)
    throws AsyncApiException, IOException {

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    JobInfo job = createJob(bulkConnection, sObjectDescriptor.getName());

    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      bulkConnection.createBatchFromStream(job, bout);
    }

    return bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();
  }

  /**
   * Wait until a batch with given batchId succeeds, or throw an exception
   *
   * @param bulkConnection bulk connection instance
   * @param jobId a job id
   * @param batchId a batch id
   * @return an input stream which represents a current batch response, which is a bunch of lines in csv format.
   *
   * @throws AsyncApiException  if there is an issue creating the job
   * @throws InterruptedException sleep interrupted
   */
  public static InputStream waitForBatchResults(BulkConnection bulkConnection, String jobId, String batchId)
    throws AsyncApiException, InterruptedException {

    BatchInfo info = null;
    for (int i = 0; i < GET_BATCH_RESULTS_TRIES; i++) {
      info = bulkConnection.getBatchInfo(jobId, batchId);

      if (info.getState() == BatchStateEnum.Completed) {
        QueryResultList list =
          bulkConnection.getQueryResultList(jobId, batchId);
        String[] resultIds = list.getResult();

        List<InputStream> streams = new ArrayList<>(resultIds.length);
        for (String resultId : resultIds) {
          streams.add(bulkConnection.getQueryResultStream(jobId, batchId, resultId));
        }

        return new SequenceInputStream(Collections.enumeration(streams));
      } else if (info.getState() == BatchStateEnum.Failed) {

        throw new BulkAPIBatchException("Batch failed", info);
      } else {
        Thread.sleep(GET_BATCH_RESULTS_SLEEP_MS);
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", info);
  }
}
