/*
 * Copyright © 2022 Cask Data, Inc.
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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;

public class SalesforceBulkUtilTest {

  @Test
  public void testCreateJob() throws AsyncApiException {
    JobInfo jobInfo = Mockito.mock(JobInfo.class);
    Mockito.when(jobInfo.getId()).thenReturn("42");
    BulkConnection bulkConnection = Mockito.mock(BulkConnection.class);
    Mockito.when(bulkConnection.getJobStatus(Mockito.any())).thenReturn(new JobInfo());
    Mockito.when(bulkConnection.createJob(Mockito.any())).thenReturn(jobInfo);
    SalesforceBulkUtil.createJob(bulkConnection, "S Object", OperationEnum.insert, "External Id Field");
    Mockito.verify(bulkConnection).createJob(Mockito.any());
    Mockito.verify(bulkConnection).getJobStatus(Mockito.any());
    Mockito.verify(jobInfo, Mockito.atLeast(1)).getId();
  }

  @Test
  public void testCloseJob() throws AsyncApiException {
    BulkConnection bulkConnection = Mockito.mock(BulkConnection.class);
    Mockito.when(bulkConnection.updateJob(Mockito.any())).thenReturn(new JobInfo());
    SalesforceBulkUtil.closeJob(bulkConnection, "42");
    Mockito.verify(bulkConnection).updateJob(Mockito.any());
  }

  @Test
  public void testCheckResults() throws AsyncApiException, IOException {
    JobInfo jobInfo = new JobInfo();
    SalesforceBulkUtil.checkResults(null, jobInfo, new ArrayList<>(), true);
    Assert.assertEquals(0L, jobInfo.getApexProcessingTime());
    Assert.assertEquals(0L, jobInfo.getTotalProcessingTime());
    Assert.assertEquals(0, jobInfo.getNumberRetries());
    Assert.assertEquals(0, jobInfo.getNumberRecordsProcessed());
    Assert.assertEquals(0, jobInfo.getNumberRecordsFailed());
    Assert.assertEquals(0, jobInfo.getNumberBatchesTotal());
    Assert.assertEquals(0, jobInfo.getNumberBatchesQueued());
    Assert.assertEquals(0, jobInfo.getNumberBatchesInProgress());
    Assert.assertEquals(0, jobInfo.getNumberBatchesFailed());
    Assert.assertEquals(0, jobInfo.getNumberBatchesCompleted());
    Assert.assertFalse(jobInfo.getFastPathEnabled());
    Assert.assertEquals(0.0, jobInfo.getApiVersion(), 0.0);
    Assert.assertEquals(0L, jobInfo.getApiActiveProcessingTime());
  }
}
