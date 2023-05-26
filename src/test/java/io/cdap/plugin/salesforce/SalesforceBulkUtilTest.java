/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.JobInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SalesforceBulkUtil}.
 */
public class SalesforceBulkUtilTest {

  private BulkConnection bulkConnection;
  private JobInfo job;
  private BatchInfo batchInfo;

  @Before
  public void setUp() {
    // Create mock BulkConnection, JobInfo, and BatchInfo objects
    bulkConnection = mock(BulkConnection.class);
    job = mock(JobInfo.class);
    batchInfo = mock(BatchInfo.class);
    when(job.getId()).thenReturn("1");
    when(job.getConcurrencyMode()).thenReturn(ConcurrencyMode.Parallel);
    when(batchInfo.getId()).thenReturn("1");
    when(batchInfo.getState()).thenReturn(BatchStateEnum.Completed);
  }

  @Test(expected = BulkAPIBatchException.class)
  public void testAwaitCompletionBatchFailed() throws Exception {
    when(batchInfo.getState()).thenReturn(BatchStateEnum.Failed);
    BatchInfoList batchInfoList = mock(BatchInfoList.class);
    BatchInfo[] batchInfoDetail = new BatchInfo[]{batchInfo};
    when(bulkConnection.getBatchInfoList(job.getId())).thenReturn(batchInfoList);
    when(batchInfoList.getBatchInfo()).thenReturn(batchInfoDetail);

    // Call the awaitCompletion method and verify that it throws a BulkAPIBatchException when the batch fails
    SalesforceBulkUtil.awaitCompletion(bulkConnection, job, Collections.singletonList(batchInfo), false);
  }

  @Test(expected = AsyncApiException.class)
  public void testAwaitCompletionAsyncApiException() throws Exception {
    when(bulkConnection.getBatchInfoList(job.getId())).thenThrow(
      new AsyncApiException("Failed to get batch info list ", AsyncExceptionCode.ClientInputError));
    // Call the awaitCompletion method and verify that it throws a AsyncApiException after retrying 10 times.
    SalesforceBulkUtil.awaitCompletion(bulkConnection, job, Collections.singletonList(batchInfo), true);
  }

  @Test
  public void testAwaitCompletionBatchCompleted() throws Exception {
    when(batchInfo.getState()).thenReturn(BatchStateEnum.Completed);
    BatchInfoList batchInfoList = mock(BatchInfoList.class);
    BatchInfo[] batchInfoDetail = new BatchInfo[]{batchInfo};
    when(bulkConnection.getBatchInfoList(job.getId())).thenReturn(batchInfoList);
    when(batchInfoList.getBatchInfo()).thenReturn(batchInfoDetail);

    // Call the awaitCompletion method and verify that it completes successfully
    SalesforceBulkUtil.awaitCompletion(bulkConnection, job, Collections.singletonList(batchInfo), true);
  }
}
