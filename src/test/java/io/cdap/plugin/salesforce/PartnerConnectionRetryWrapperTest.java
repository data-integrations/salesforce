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
package io.cdap.plugin.salesforce;

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.PartnerConnectionRetryWrapper;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceQueryExecutionException;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSplitUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SalesforceSplitUtil.class, PartnerConnectionRetryWrapper.class})
public class PartnerConnectionRetryWrapperTest {
  @Mock
  private PartnerConnection mockConnection;
  @Mock
  private PartnerConnectionRetryWrapper mockConnectionWrapper;

  @Before
  public void setUp() throws Exception {
    PowerMockito.mockStatic(SalesforceSplitUtil.class);
    RetryPolicy<Object> dummyPolicy = RetryPolicy.builder().withMaxRetries(2).build();
    when(SalesforceSplitUtil.getRetryPolicy(anyLong(), anyLong(), anyInt(), anyBoolean())).thenReturn(dummyPolicy);
    when(mockConnectionWrapper.isRetryableConnectionError(any(ConnectionException.class)))
      .thenReturn(true);

    ConnectorConfig config = new ConnectorConfig();
    config.setServiceEndpoint("https://login.salesforce.com/services/Soap/u/59.0");
    config.setManualLogin(true);
    mockConnection = mock(PartnerConnection.class);
    when(mockConnection.getConfig()).thenReturn(config);
    mockConnectionWrapper = new PartnerConnectionRetryWrapper(mockConnection, dummyPolicy);
  }

  @Test
  public void testQueryWithRetry_RetriesThenSucceeds() throws Exception {
    ConnectionException retryable = new ConnectionException("Connection refused");
    QueryResult successResult = mock(QueryResult.class);
    // Simulate two failures followed by a success
    when(mockConnection.query("SELECT Id FROM Account"))
      .thenThrow(retryable)
      .thenThrow(retryable)
      .thenReturn(successResult);
    QueryResult result = mockConnectionWrapper.query("SELECT Id FROM Account");
    assertEquals(successResult, result);
    verify(mockConnection, times(3)).query("SELECT Id FROM Account");
  }

  @Test
  public void testQueryWithRetry_MaxRetriesExceeded() throws Exception {
    ConnectionException retryable = new ConnectionException("Connection timed out");
    when(mockConnection.query("SELECT Id FROM Account")).thenThrow(retryable);
    try {
      mockConnectionWrapper.query("SELECT Id FROM Account");
    } catch (FailsafeException e) {
      assertTrue(e.getCause() instanceof SalesforceQueryExecutionException);
      assertTrue(e.getCause().getCause() instanceof ConnectionException);
      verify(mockConnection, times(3)).query("SELECT Id FROM Account");
    }
  }

  @Test
  public void testDescribeGlobalWithRetry_RetriesThenSucceeds() throws Exception {
    ConnectionException retryable = new ConnectionException("Socket timeout");
    DescribeGlobalResult expected = mock(DescribeGlobalResult.class);
    when(mockConnection.describeGlobal())
      .thenThrow(retryable)
      .thenThrow(retryable)
      .thenReturn(expected);
    DescribeGlobalResult result = mockConnectionWrapper.describeGlobal();
    assertEquals(expected, result);
    verify(mockConnection, times(3)).describeGlobal();
  }

  @Test
  public void testDescribeGlobalWithRetry_MaxRetriesExceeded() throws Exception {
    ConnectionException retryable = new ConnectionException("Connection timed out");
    when(mockConnection.describeGlobal()).thenThrow(retryable);
    try {
      mockConnectionWrapper.describeGlobal();
    } catch (FailsafeException e) {
      assertTrue(e.getCause() instanceof SalesforceQueryExecutionException);
      assertTrue(e.getCause().getCause() instanceof ConnectionException);
      verify(mockConnection, times(3)).describeGlobal();
    }
  }

  @Test
  public void testQueryMoreWithRetry_RetriesThenSucceeds() throws Exception {
    ConnectionException retryable = new ConnectionException("Connection refused");
    QueryResult expected = mock(QueryResult.class);
    when(mockConnection.queryMore("abc123"))
      .thenThrow(retryable)
      .thenReturn(expected);
    QueryResult result = mockConnectionWrapper.queryMore("abc123");

    assertEquals(expected, result);
    verify(mockConnection, times(2)).queryMore("abc123");
  }

  @Test
  public void testDescribeSObjectsWithRetry_RetriesThenSucceeds() throws Exception {
    ConnectionException retryable = new ConnectionException("Socket timeout");
    DescribeSObjectResult[] expected = new DescribeSObjectResult[]{
      mock(DescribeSObjectResult.class)
    };

    when(mockConnection.describeSObjects(new String[]{"Account"}))
      .thenThrow(retryable)
      .thenReturn(expected);

    DescribeSObjectResult[] result = mockConnectionWrapper.describeSObjects(new String[]{"Account"});

    assertArrayEquals(expected, result);
    verify(mockConnection, times(2)).describeSObjects(new String[]{"Account"});
  }


  @Test
  public void testGetUserInfoWithRetry_RetriesThenSucceeds() throws Exception {
    ConnectionException retryable = new ConnectionException("Intermittent error");
    GetUserInfoResult userInfo = mock(GetUserInfoResult.class);
    when(userInfo.getOrganizationId()).thenReturn("org123");
    when(mockConnection.getUserInfo())
      .thenThrow(retryable)
      .thenReturn(userInfo);
    GetUserInfoResult result = mockConnectionWrapper.getUserInfo();

    assertEquals("org123", result.getOrganizationId());
    verify(mockConnection, times(2)).getUserInfo();
  }

  @Test
  public void testRetrieveWithRetry_RetryAndSuccess() throws Exception {
    ConnectionException retryable = new ConnectionException("Server unavailable");
    SObject obj = new SObject();
    SObject[] expected = new SObject[]{obj};
    when(mockConnection.retrieve("Id,Name", "Account", new String[]{"001xx000003DGbY"}))
      .thenThrow(retryable)
      .thenReturn(expected);
    SObject[] result = mockConnectionWrapper.retrieve("Id,Name", "Account", new String[]{"001xx000003DGbY"});
    assertArrayEquals(expected, result);
    verify(mockConnection, times(2)).retrieve("Id,Name", "Account", new String[]{"001xx000003DGbY"});
  }
}
