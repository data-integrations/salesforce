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

package io.cdap.plugin.salesforce.plugin.source.streaming;

import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SalesforceStreamingSource}.
 */
public class SalesforceStreamingSourceConfigTest {

  @Test
  public void testGetQueryForPushTopicQuery() throws NoSuchFieldException {
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", "sObjectName", "securityToken", oAuthInfo);

    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("pushTopicQuery"), "pushTopicQuery");

    assertEquals("pushTopicQuery", config.getQuery());
  }

  @Test
  public void testGetQueryOnNull() throws NoSuchFieldException {
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", null, "securityToken", oAuthInfo);

    assertEquals(null, config.getQuery());
  }

  /*@Test
  public void testGetQueryForSObjectName() throws Exception {
    SalesforceStreamingSourceConfig config = mock(SalesforceStreamingSourceConfig.class);
    SObjectDescriptor sObjectDescriptorObj = mock(SObjectDescriptor.class);
    FieldSetter.setField(config, SalesforceStreamingSourceConfig.class.getDeclaredField("sObjectName"), "sObjectName");
    when(SObjectDescriptor.fromName(any(), any(), any())).thenReturn(sObjectDescriptorObj);
    when(SalesforceQueryUtil.createSObjectQuery(any(),any(),any())).thenReturn("sObjectName");

    assertEquals("sObjectName", config.getQuery());
  }*/

  @Test
  public void testEnsurePushTopicExistAndWithCorrectFields() {
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "url");
    SalesforceStreamingSourceConfig config = new SalesforceStreamingSourceConfig("referenceName", "consumerKey",
      "consumerSecret", "username", "password", "loginUrl", "pushTopicName", "sObjectName", "securityToken", oAuthInfo);

    when()
    config.ensurePushTopicExistAndWithCorrectFields();
  }

}
