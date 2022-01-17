/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;

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
      "Reference Name", "Consumer Key", "Consumer Secret",
      "username", "password", "https://example.org/example",
      "Push Topic Name", "S Object Name", "token", oAuthInfo);
  }

  @Test
  public void testConstructor() {
    Assert.assertEquals("Push Topic Name", actualSalesforceStreamingSourceConfig.getPushTopicName());
    Assert.assertNull(actualSalesforceStreamingSourceConfig.getPushTopicNotifyForFields());
    Assert.assertNull(actualSalesforceStreamingSourceConfig.getPushTopicQuery());
    Assert.assertEquals("Reference Name", actualSalesforceStreamingSourceConfig.referenceName);
    Assert.assertEquals("Consumer Key", actualSalesforceStreamingSourceConfig.getConsumerKey());
    Assert.assertEquals("https://example.org/example", actualSalesforceStreamingSourceConfig.getLoginUrl());
    Assert.assertEquals("Push Topic Name", actualSalesforceStreamingSourceConfig.getPushTopicName());
    Assert.assertSame(oAuthInfo, actualSalesforceStreamingSourceConfig.getOAuthInfo());
    Assert.assertEquals("Consumer Secret", actualSalesforceStreamingSourceConfig.getConsumerSecret());
    Assert.assertEquals("passwordtoken", actualSalesforceStreamingSourceConfig.getPassword());
    Assert.assertEquals("username", actualSalesforceStreamingSourceConfig.getUsername());
  }

  @Test
  public void testFetchPushTopicByNameInvalidFieldName()
    throws ConnectionException {
    PartnerConnection partnerConnection = Mockito.mock(PartnerConnection.class);
    try {
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, ",");
      Assert.fail("Exception is not thrown for valid pushTopicNames");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Push topic name ',' can only contain latin letters.", e.getMessage());
    }
  }

  @Test
  public void testFetchPushTopicByNameTestWhenZeroRecord()
    throws ConnectionException {
    SObject[] sObjectArray = {};
    QueryResult queryResult = Mockito.mock(QueryResult.class);
    PartnerConnection partnerConnection = Mockito.mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(ArgumentMatchers.anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    Assert.assertEquals(null, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

  @Test
  public void fetchPushTopicByNameTestWhenSingleRecord() throws ConnectionException {
    SObject sObject = Mockito.mock(SObject.class);
    SObject[] sObjectArray = {sObject};
    QueryResult queryResult = Mockito.mock(QueryResult.class);
    PartnerConnection partnerConnection = Mockito.mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(ArgumentMatchers.anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    Assert.assertEquals(sObject, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

  @Test
  public void fetchPushTopicByNameTestWhenMultipleRecord() throws ConnectionException {
    SObject sObject = Mockito.mock(SObject.class);
    SObject[] sObjectArray = {sObject, sObject};
    QueryResult queryResult = Mockito.mock(QueryResult.class);
    PartnerConnection partnerConnection = Mockito.mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(ArgumentMatchers.anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    try {
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test");
      Assert.fail("Exception is not thrown for valid pushTopicNames");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Excepted one or zero pushTopics with name = 'test' found 2", e.getMessage());
    }
  }

  @Test
  public void getSObjectQueryTest() throws Exception {
    PowerMockito.mockStatic(SObjectDescriptor.class);
    PowerMockito.mockStatic(SalesforceQueryUtil.class);
    PowerMockito.mockStatic(SObjectFilterDescriptor.class);
    SObjectDescriptor sObjectDescriptor = Mockito.spy(new SObjectDescriptor("test", new ArrayList<>()));
    SObjectFilterDescriptor sObjectFilterDescriptor = Mockito.mock(SObjectFilterDescriptor.class);
    SalesforceStreamingSourceConfig config = Mockito.mock(SalesforceStreamingSourceConfig.class);
    PowerMockito.when(config.canAttemptToEstablishConnection()).thenReturn(true);
    PowerMockito.when(sObjectDescriptor.getFieldsNames()).thenReturn(new ArrayList<>());
    PowerMockito.when(SObjectFilterDescriptor.noOp()).thenReturn(sObjectFilterDescriptor);
    PowerMockito.when(SObjectDescriptor.fromName(ArgumentMatchers.anyString(), ArgumentMatchers.any(),
                                                 ArgumentMatchers.any())).thenReturn(sObjectDescriptor);
    PowerMockito.when(SalesforceQueryUtil.createSObjectQuery(ArgumentMatchers.any(), ArgumentMatchers.anyString()
      , ArgumentMatchers.any())).thenReturn(
      "sObjectQuery");
    try {
      Whitebox.invokeMethod(config, "getSObjectQuery");
      Assert.fail("Exception is not thrown.");
    } catch (Exception e) {
      e.getMessage();
    }
  }

  @Test
  public void getSObjectQueryTestWhencanAttemptToEstablishConnectionIsFalse() throws Exception {
    SalesforceStreamingSourceConfig config = Mockito.mock(SalesforceStreamingSourceConfig.class);
    Assert.assertNull(Whitebox.invokeMethod(config, "getSObjectQuery"));
  }

}
