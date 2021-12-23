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

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SalesforceBulkUtilTest {
  @Test
  public void testCreateJob() throws AsyncApiException {
    JobInfo jobInfo = mock(JobInfo.class);
    when(jobInfo.getId()).thenReturn("42");
    BulkConnection bulkConnection = mock(BulkConnection.class);
    when(bulkConnection.getJobStatus((String) any())).thenReturn(new JobInfo());
    when(bulkConnection.createJob((JobInfo) any())).thenReturn(jobInfo);
    SalesforceBulkUtil.createJob(bulkConnection, "S Object", OperationEnum.insert, "External Id Field");
    verify(bulkConnection).createJob((JobInfo) any());
    verify(bulkConnection).getJobStatus((String) any());
    verify(jobInfo, atLeast(1)).getId();
  }

  @Test
  public void testCloseJob() throws AsyncApiException {
    BulkConnection bulkConnection = mock(BulkConnection.class);
    when(bulkConnection.updateJob((JobInfo) any())).thenReturn(new JobInfo());
    SalesforceBulkUtil.closeJob(bulkConnection, "42");
    verify(bulkConnection).updateJob((JobInfo) any());
  }

  @Test
  public void testCheckResults() throws AsyncApiException, IOException {
    JobInfo jobInfo = new JobInfo();
    SalesforceBulkUtil.checkResults(null, jobInfo, new ArrayList<>(), true);
    assertEquals(0L, jobInfo.getApexProcessingTime());
    assertEquals(0L, jobInfo.getTotalProcessingTime());
    assertEquals(0, jobInfo.getNumberRetries());
    assertEquals(0, jobInfo.getNumberRecordsProcessed());
    assertEquals(0, jobInfo.getNumberRecordsFailed());
    assertEquals(0, jobInfo.getNumberBatchesTotal());
    assertEquals(0, jobInfo.getNumberBatchesQueued());
    assertEquals(0, jobInfo.getNumberBatchesInProgress());
    assertEquals(0, jobInfo.getNumberBatchesFailed());
    assertEquals(0, jobInfo.getNumberBatchesCompleted());
    assertFalse(jobInfo.getFastPathEnabled());
    assertEquals(0.0, jobInfo.getApiVersion(), 0.0);
    assertEquals(0L, jobInfo.getApiActiveProcessingTime());
  }
}

