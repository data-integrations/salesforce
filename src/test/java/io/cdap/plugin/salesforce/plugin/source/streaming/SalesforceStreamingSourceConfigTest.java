/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package io.cdap.plugin.salesforce.plugin.source.streaming;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectFilterDescriptor;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Tests for SalesforceStreamingSourceConfig
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SalesforceStreamingSourceConfig.class, SalesforceStreamingSource.class, SObjectDescriptor.class,
  SalesforceQueryUtil.class, SObjectFilterDescriptor.class})
public class SalesforceStreamingSourceConfigTest {

  SalesforceStreamingSourceConfig actualSalesforceStreamingSourceConfig;
  OAuthInfo oAuthInfo;

  @Before
  public void setUp() {

    oAuthInfo = new OAuthInfo("token", "https://d5j000001ufckeay.lightning.force.com");

    actualSalesforceStreamingSourceConfig = new SalesforceStreamingSourceConfig(
      "Reference Name", "Consumer Key", "Consumer Secret", "username", "password", "https://example.org/example",
      "Push Topic Name", "S Object Name", "token", oAuthInfo);
  }

  @Test
  public void testConstructor() {
    assertEquals("Push Topic Name", actualSalesforceStreamingSourceConfig.getPushTopicName());
    assertNull(actualSalesforceStreamingSourceConfig.getPushTopicNotifyForFields());
    assertNull(actualSalesforceStreamingSourceConfig.getPushTopicQuery());
    assertEquals("Reference Name", actualSalesforceStreamingSourceConfig.referenceName);
    assertEquals("Consumer Key", actualSalesforceStreamingSourceConfig.getConsumerKey());
    assertEquals("https://example.org/example", actualSalesforceStreamingSourceConfig.getLoginUrl());
    assertEquals("Push Topic Name", actualSalesforceStreamingSourceConfig.getPushTopicName());
    assertSame(oAuthInfo, actualSalesforceStreamingSourceConfig.getOAuthInfo());
    assertEquals("Consumer Secret", actualSalesforceStreamingSourceConfig.getConsumerSecret());
    assertEquals("passwordtoken", actualSalesforceStreamingSourceConfig.getPassword());
    assertEquals("username", actualSalesforceStreamingSourceConfig.getUsername());
  }

  @Test
  public void testFetchPushTopicByNameInvalidFieldName()
    throws ConnectionException {
    PartnerConnection partnerConnection = mock(PartnerConnection.class);
    try {
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, ",");
    } catch (IllegalArgumentException e) {
      assertEquals("Push topic name ',' can only contain latin letters.", e.getMessage());
    }
  }

  @Test
  public void testFetchPushTopicByNameTestWhenZeroRecord()
    throws ConnectionException {
    SObject[] sObjectArray = {};
    QueryResult queryResult = mock(QueryResult.class);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    assertEquals(null, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

  @Test
  public void fetchPushTopicByNameTestWhenSingleRecord() throws ConnectionException {
    SObject sObject = mock(SObject.class);
    SObject[] sObjectArray = {sObject};
    QueryResult queryResult = mock(QueryResult.class);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    assertEquals(sObject, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

  @Test
  public void fetchPushTopicByNameTestWhenMultipleRecord() throws ConnectionException {
    SObject sObject = mock(SObject.class);
    SObject[] sObjectArray = {sObject, sObject};
    QueryResult queryResult = mock(QueryResult.class);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    try {
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test");
    } catch (IllegalStateException e) {
      assertEquals("Excepted one or zero pushTopics with name = 'test' found 2", e.getMessage());
    }
  }

  @Test
  public void getSObjectQueryTest() throws Exception {
    mockStatic(SObjectDescriptor.class);
    mockStatic(SalesforceQueryUtil.class);
    mockStatic(SObjectFilterDescriptor.class);
    SObjectDescriptor sObjectDescriptor = spy(new SObjectDescriptor("test", new ArrayList<>()));
    SObjectFilterDescriptor sObjectFilterDescriptor = mock(SObjectFilterDescriptor.class);
    SalesforceStreamingSourceConfig config = mock(SalesforceStreamingSourceConfig.class);
    PowerMockito.when(config.canAttemptToEstablishConnection()).thenReturn(true);
    PowerMockito.when(sObjectDescriptor.getFieldsNames()).thenReturn(new ArrayList<>());
    PowerMockito.when(SObjectFilterDescriptor.noOp()).thenReturn(sObjectFilterDescriptor);
    PowerMockito.when(SObjectDescriptor.fromName(anyString(), any(), any())).thenReturn(sObjectDescriptor);
    PowerMockito.when(SalesforceQueryUtil.createSObjectQuery(any(), anyString(), any())).thenReturn("sObjectQuery");
    try {
      Whitebox.invokeMethod(config, "getSObjectQuery");
    } catch (Exception e) {
      e.getMessage();
    }
  }

  @Test
  public void getSObjectQueryTestWhencanAttemptToEstablishConnectionIsFalse() throws Exception {
    SalesforceStreamingSourceConfig config = mock(SalesforceStreamingSourceConfig.class);
    assertNull(Whitebox.invokeMethod(config, "getSObjectQuery"));
  }


}
