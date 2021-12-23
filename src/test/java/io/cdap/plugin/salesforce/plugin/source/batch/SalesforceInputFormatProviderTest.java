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
package io.cdap.plugin.salesforce.plugin.source.batch;

import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SalesforceInputFormatProviderTest {

  SalesforceMultiSourceConfig salesforceMultiSourceConfig;
  OAuthInfo oAuthInfo;

  @Before
  public void setUp() {
    oAuthInfo = new OAuthInfo("token", "https://example.org/example");

    salesforceMultiSourceConfig = new SalesforceMultiSourceConfig("Reference Name",
      "Consumer Key", "Consumer Secret", "username", "password", "https://example.org/example", "2020-03-01",
      "2020-03-01", "Duration", "Offset", "White List", "Black List", "S Object Name Field", "token", oAuthInfo);
  }

  @Test
  public void testInputFormatProvider() {
    HashMap<String, String> stringStringMap = new HashMap<>(1);
    List<String> salesforceSplitList = new ArrayList<>();
    SalesforceInputFormatProvider actualSalesforceInputFormatProvider = new SalesforceInputFormatProvider(
      salesforceMultiSourceConfig, salesforceSplitList, stringStringMap, "S Object Name Field");

    assertEquals("io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat",
      actualSalesforceInputFormatProvider.getInputFormatClassName());
    Map<String, String> inputFormatConfiguration = actualSalesforceInputFormatProvider.getInputFormatConfiguration();
    assertEquals(5, inputFormatConfiguration.size());
    assertEquals("{}", inputFormatConfiguration.get("mapred.salesforce.input.schemas"));
    assertEquals("token", inputFormatConfiguration.get("mapred.salesforce.oauth.token"));
    assertEquals("https://example.org/example", inputFormatConfiguration.get("mapred.salesforce.oauth.instance.url"));
    assertEquals("S Object Name Field", inputFormatConfiguration.get("mapred.salesforce.input.sObjectNameField"));
    assertEquals("Reference Name", salesforceMultiSourceConfig.referenceName);
    Set<String> blackList = salesforceMultiSourceConfig.getBlackList();
    assertEquals(1, blackList.size());
    assertTrue(blackList.contains("Black List"));
    assertEquals("Consumer Secret", salesforceMultiSourceConfig.getConsumerSecret());
    OAuthInfo oAuthInfo1 = salesforceMultiSourceConfig.getOAuthInfo();
    assertSame(oAuthInfo, oAuthInfo1);
    assertEquals("passwordtoken", salesforceMultiSourceConfig.getPassword());
    assertEquals("2020-03-01", salesforceMultiSourceConfig.getDatetimeAfter());
    PluginProperties properties = salesforceMultiSourceConfig.getProperties();
    PluginProperties rawProperties = salesforceMultiSourceConfig.getRawProperties();
    assertEquals(properties, rawProperties);
    assertEquals("Consumer Key", salesforceMultiSourceConfig.getConsumerKey());
    assertEquals("2020-03-01", salesforceMultiSourceConfig.getDatetimeBefore());
    assertEquals("S Object Name Field", salesforceMultiSourceConfig.getSObjectNameField());
    assertEquals("username", salesforceMultiSourceConfig.getUsername());
    assertEquals("https://example.org/example", salesforceMultiSourceConfig.getLoginUrl());
    Set<String> whiteList = salesforceMultiSourceConfig.getWhiteList();
    assertEquals(1, whiteList.size());
    assertTrue(whiteList.contains("White List"));
    AuthenticatorCredentials authenticatorCredentials = salesforceMultiSourceConfig.getAuthenticatorCredentials();
    assertNull(authenticatorCredentials.getConsumerKey());
    assertEquals("PluginProperties{properties={}, macros=Macros{lookupProperties=[], macroFunctions=[]}}",
      rawProperties.toString());
    assertNull(authenticatorCredentials.getConsumerSecret());
    assertSame(oAuthInfo1, authenticatorCredentials.getOAuthInfo());
    assertNull(authenticatorCredentials.getUsername());
    assertEquals("PluginProperties{properties={}, macros=Macros{lookupProperties=[], macroFunctions=[]}}",
      properties.toString());
    assertEquals("https://example.org/example", oAuthInfo1.getInstanceURL());
    assertNull(authenticatorCredentials.getLoginUrl());
    assertNull(authenticatorCredentials.getPassword());
    assertEquals("token", oAuthInfo1.getAccessToken());
    Map<String, String> props = properties.getProperties();
    assertTrue(props.isEmpty());
    Macros macros = rawProperties.getMacros();
    assertTrue(macros.getLookups().isEmpty());
    assertTrue(macros.getMacroFunctions().isEmpty());
    assertEquals("Macros{lookupProperties=[], macroFunctions=[]}", macros.toString());
    assertTrue(stringStringMap.isEmpty());
    assertTrue(salesforceSplitList.isEmpty());
  }

  @Test
  public void testInputFormatProviderWithOuathInfoAsNull() {
    HashMap<String, String> stringStringMap = new HashMap<>(1);
    List<String> salesforceSplitList = new ArrayList<>();
    salesforceMultiSourceConfig = new SalesforceMultiSourceConfig("Reference Name",
      "Consumer Key", "Consumer Secret", "username", "password", "https://example.org/example", "2020-03-01",
      "2020-03-01", "Duration", "Offset", "White List", "Black List", "S Object Name Field", "token", null);

    SalesforceInputFormatProvider actualSalesforceInputFormatProvider = new SalesforceInputFormatProvider(
      salesforceMultiSourceConfig, salesforceSplitList, stringStringMap, "S Object Name Field");
  }


  @Test
  public void testCInputFormatConfigurationSize() {
    HashMap<String, String> schemas = new HashMap<>(1);

    SalesforceInputFormatProvider salesforceInputFormatProvider = new SalesforceInputFormatProvider
      (salesforceMultiSourceConfig, new ArrayList<>(), schemas,
        "S Object Name Field");
    assertEquals(5, salesforceInputFormatProvider.getInputFormatConfiguration().size());
  }

  @Test
  public void testGetInputFormatClassName() {
    HashMap<String, String> schemas = new HashMap<>(1);
    assertEquals("io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat",
      (new SalesforceInputFormatProvider(salesforceMultiSourceConfig, new ArrayList<>(), schemas,
        "S Object Name Field"))
        .getInputFormatClassName());
  }

}

