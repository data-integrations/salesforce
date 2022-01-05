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
import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

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
  public void testFetchPushTopicByNameTestWhenZeroRecord()
    throws ConnectionException {
    SObject[] sObjectArray = {};
    QueryResult queryResult = mock(QueryResult.class);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);
    Mockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    Mockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    assertEquals(null, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

}
