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
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectFilterDescriptor;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SalesforceStreamingSourceConfig.class, SObjectDescriptor.class, SalesforceQueryUtil.class,
  SObjectFilterDescriptor.class})
public class SalesforceStreamingSourceConfigTest {

  @Test
  public void testGetQueryForPushTopicQuery() throws NoSuchFieldException {
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", "sObjectName", "securityToken", oAuthInfo);

    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicQuery"),
      "pushTopicQuery");

    assertEquals("pushTopicQuery", config.getQuery());
  }

  @Test
  public void testGetQueryOnNull() throws NoSuchFieldException {
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);

    assertEquals(null, config.getQuery());
  }

  @Test
  public void testGetQueryForSObjectName() throws Exception {
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    /*SalesforceStreamingSourceConfig salesforceStreamingSourceConfig = new SalesforceStreamingSourceConfig(
      "referenceName", "consumerKey", "consumerSecret", "username", "password", "loginUrl", "pushTopicName",
      null, "securityToken", oAuthInfo);*/
    //SalesforceStreamingSourceConfig config = spy(salesforceStreamingSourceConfig);
    SalesforceStreamingSourceConfig salesforceStreamingSourceConfig =
      PowerMockito.spy(new SalesforceStreamingSourceConfig(
        "referenceName", "consumerKey", "consumerSecret", "username", "password", "loginUrl", "pushTopicName",
        null, "securityToken", oAuthInfo));
    FieldSetter.setField(salesforceStreamingSourceConfig,
      SalesforceStreamingSourceConfig.class.getDeclaredField("sObjectName"), "sObjectName");
    //when(SObjectDescriptor.fromName(any(), any(), any())).thenReturn(sObjectDescriptorObj);
    //when(SalesforceQueryUtil.createSObjectQuery(any(),any(),any())).thenReturn("sObjectName");
    //doReturn("test").when(config, "")
    //when(salesforceStreamingSourceConfig, "getSObjectQuery").thenReturn("test");
    PowerMockito.doReturn("sObjectName").when(salesforceStreamingSourceConfig, "getSObjectQuery");
    assertEquals("sObjectName", salesforceStreamingSourceConfig.getQuery());
  }

  @Test
  public void testEnsurePushTopicExistAndWithCorrectFields() throws Exception {
    SObject sObject = mock(SObject.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", "sObjectName", "securityToken", oAuthInfo);
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicQuery"),
      "pushTopicQuery");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyCreate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyUpdate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyDelete"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyForFields"),
      "Enabled");
    PowerMockito.mockStatic(SalesforceStreamingSourceConfig.class);
    PowerMockito.when(SalesforceStreamingSourceConfig.fetchPushTopicByName(any(), anyString())).thenReturn(sObject);
    config.ensurePushTopicExistAndWithCorrectFields();
  }

  @Test
  public void testEnsurePushTopicExistAndWithCorrectFieldsOnQueryNull() throws Exception {
    SObject sObject = mock(SObject.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyCreate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyUpdate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyDelete"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyForFields"),
      "Enabled");
    PowerMockito.mockStatic(SalesforceStreamingSourceConfig.class);
    PowerMockito.when(SalesforceStreamingSourceConfig.fetchPushTopicByName(any(), anyString())).thenReturn(sObject);
    config.ensurePushTopicExistAndWithCorrectFields();
  }

  @Test
  public void testEnsurePushTopicExistAndWithCorrectFieldsOnSObjectNullOnInvalidConfigException() throws Exception {
    SObject sObject = mock(SObject.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyCreate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyUpdate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyDelete"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyForFields"),
      "Enabled");
    PowerMockito.mockStatic(SalesforceStreamingSourceConfig.class);
    PowerMockito.when(SalesforceStreamingSourceConfig.fetchPushTopicByName(any(), anyString())).thenReturn(null);
    try {
      config.ensurePushTopicExistAndWithCorrectFields();
    } catch (InvalidConfigException e) {
      e.getMessage();
    }
  }

  @Test
  public void testEnsurePushTopicExistAndWithCorrectFieldsOnPushTopicNull() throws Exception {
    SObject sObject = mock(SObject.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicQuery"),
      "pushTopicQuery");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyCreate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyUpdate"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyDelete"),
      "Enabled");
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicNotifyForFields"),
      "Enabled");
    PowerMockito.mockStatic(SalesforceStreamingSourceConfig.class);
    PowerMockito.when(SalesforceStreamingSourceConfig.fetchPushTopicByName(any(), anyString())).thenReturn(null);
    try {
      config.ensurePushTopicExistAndWithCorrectFields();
    } catch (InvalidStageException e) {
      e.getMessage();
    }
  }

  @Test
  public void fetchPushTopicByNameTestOnIllegalArgumentException() throws ConnectionException {
    SalesforceStreamingSourceConfig config = mock(SalesforceStreamingSourceConfig.class);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);
    try {
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "");
    } catch (IllegalArgumentException e) {
      assertEquals("Push topic name '' can only contain latin letters.", e.getMessage());
    }
  }

  @Test
  public void fetchPushTopicByNameTestWhenOneRecord() throws ConnectionException, NoSuchFieldException {
    SObject sObject = mock(SObject.class);
    SObject[] sObjectArray = {sObject};
    QueryResult queryResult = mock(QueryResult.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);

    PowerMockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    PowerMockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    assertEquals(sObject, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

  @Test
  public void fetchPushTopicByNameTestWhenZeroRecord() throws ConnectionException, NoSuchFieldException {
    SObject sObject = mock(SObject.class);
    SObject[] sObjectArray = {};
    QueryResult queryResult = mock(QueryResult.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);

    PowerMockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    PowerMockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    assertEquals(null, SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test"));
  }

  @Test
  public void fetchPushTopicByNameTestWhenMultipleRecord() throws ConnectionException, NoSuchFieldException {
    SObject sObject = mock(SObject.class);
    SObject[] sObjectArray = {sObject, sObject};
    QueryResult queryResult = mock(QueryResult.class);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);
    PartnerConnection partnerConnection = mock(PartnerConnection.class);

    PowerMockito.when(partnerConnection.query(anyString())).thenReturn(queryResult);
    PowerMockito.when(queryResult.getRecords()).thenReturn(sObjectArray);
    try {
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, "test");
    } catch (IllegalStateException e) {
      assertEquals("Excepted one or zero pushTopics with name = 'test' found 2", e.getMessage());
    }
  }

  @Test
  public void getSObjectQueryTest() throws Exception {
    PowerMockito.mockStatic(SObjectDescriptor.class);
    PowerMockito.mockStatic(SalesforceQueryUtil.class);
    PowerMockito.mockStatic(SObjectFilterDescriptor.class);
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
