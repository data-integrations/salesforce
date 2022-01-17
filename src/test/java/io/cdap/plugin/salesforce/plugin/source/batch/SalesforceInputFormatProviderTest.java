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
package io.cdap.plugin.salesforce.plugin.source.batch;

import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SalesforceInputFormatProviderTest {

  SalesforceMultiSourceConfig salesforceMultiSourceConfig;
  OAuthInfo oAuthInfo;

  @Before
  public void setUp() {
    oAuthInfo = new OAuthInfo("token", "https://example.org/example");

    salesforceMultiSourceConfig = new SalesforceMultiSourceConfig("Reference Name",
                                                                  "Consumer Key", "Consumer Secret", "username", "password",
                                                                  "https://example.org/example", "2020-03-01",
                                                                  "2020-03-01", "Duration", "Offset", "White List", "" +
                                                                    "Black List", "S Object Name Field", "token", oAuthInfo);
  }

  @Test
  public void testInputFormatProvider() {
    HashMap<String, String> stringStringMap = new HashMap<>();
    List<SalesforceSplit> salesforceSplitList = new ArrayList<>();
    SalesforceInputFormatProvider actualSalesforceInputFormatProvider = new SalesforceInputFormatProvider(
      salesforceMultiSourceConfig, stringStringMap, salesforceSplitList, "S Object Name Field");

    Assert.assertEquals("io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat",
                        actualSalesforceInputFormatProvider.getInputFormatClassName());
    Map<String, String> inputFormatConfiguration = actualSalesforceInputFormatProvider.
      getInputFormatConfiguration();
    Assert.assertEquals(5, inputFormatConfiguration.size());
    Assert.assertEquals("{}", inputFormatConfiguration.get("mapred.salesforce.input.schemas"));
    Assert.assertEquals("token", inputFormatConfiguration.get("mapred.salesforce.oauth.token"));
    Assert.assertEquals("https://example.org/example",
                        inputFormatConfiguration.get("mapred.salesforce.oauth.instance.url"));
    Assert.assertEquals("S Object Name Field",
                        inputFormatConfiguration.get("mapred.salesforce.input.sObjectNameField"));
    Assert.assertEquals("Reference Name", salesforceMultiSourceConfig.referenceName);
    Set<String> blackList = salesforceMultiSourceConfig.getBlackList();
    Assert.assertEquals(1, blackList.size());
    Assert.assertTrue(blackList.contains("Black List"));
    Assert.assertEquals("Consumer Secret", salesforceMultiSourceConfig.getConsumerSecret());
    OAuthInfo oAuthInfo1 = salesforceMultiSourceConfig.getOAuthInfo();
    Assert.assertSame(oAuthInfo, oAuthInfo1);
    Assert.assertEquals("passwordtoken", salesforceMultiSourceConfig.getPassword());
    Assert.assertEquals("2020-03-01", salesforceMultiSourceConfig.getDatetimeAfter());
    PluginProperties properties = salesforceMultiSourceConfig.getProperties();
    PluginProperties rawProperties = salesforceMultiSourceConfig.getRawProperties();
    Assert.assertEquals(properties, rawProperties);
    Assert.assertEquals("Consumer Key", salesforceMultiSourceConfig.getConsumerKey());
    Assert.assertEquals("2020-03-01", salesforceMultiSourceConfig.getDatetimeBefore());
    Assert.assertEquals("S Object Name Field", salesforceMultiSourceConfig.getSObjectNameField());
    Assert.assertEquals("username", salesforceMultiSourceConfig.getUsername());
    Assert.assertEquals("https://example.org/example", salesforceMultiSourceConfig.getLoginUrl());
    Set<String> whiteList = salesforceMultiSourceConfig.getWhiteList();
    Assert.assertEquals(1, whiteList.size());
    Assert.assertTrue(whiteList.contains("White List"));
    AuthenticatorCredentials authenticatorCredentials = salesforceMultiSourceConfig.getAuthenticatorCredentials();
    Assert.assertNull(authenticatorCredentials.getConsumerKey());
    Assert.assertEquals("PluginProperties{properties={}, macros=Macros{lookupProperties=[], macroFunctions=[]}}",
                        rawProperties.toString());
    Assert.assertNull(authenticatorCredentials.getConsumerSecret());
    Assert.assertSame(oAuthInfo1, authenticatorCredentials.getOAuthInfo());
    Assert.assertNull(authenticatorCredentials.getUsername());
    Assert.assertEquals("PluginProperties{properties={}, macros=Macros{lookupProperties=[], macroFunctions=[]}}",
                        properties.toString());
    Assert.assertEquals("https://example.org/example", oAuthInfo1.getInstanceURL());
    Assert.assertNull(authenticatorCredentials.getLoginUrl());
    Assert.assertNull(authenticatorCredentials.getPassword());
    Assert.assertEquals("token", oAuthInfo1.getAccessToken());
    Map<String, String> props = properties.getProperties();
    Assert.assertTrue(props.isEmpty());
    Macros macros = rawProperties.getMacros();
    Assert.assertTrue(macros.getLookups().isEmpty());
    Assert.assertTrue(macros.getMacroFunctions().isEmpty());
    Assert.assertEquals("Macros{lookupProperties=[], macroFunctions=[]}", macros.toString());
    Assert.assertTrue(stringStringMap.isEmpty());
    Assert.assertTrue(salesforceSplitList.isEmpty());
  }

  @Test
  public void testInputFormatProviderWithOuathInfoAsNull() {
    HashMap<String, String> stringStringMap = new HashMap<>();
    List<SalesforceSplit> salesforceSplitList = new ArrayList<>();
    salesforceMultiSourceConfig = new SalesforceMultiSourceConfig("Reference Name",
                                                                  "Consumer Key", "Consumer Secret", "username", "password", "https://example.org/example", "2020-03-01",
                                                                  "2020-03-01", "Duration", "Offset", "White List", "Black List", "S Object Name Field", "token", null);

    SalesforceInputFormatProvider actualSalesforceInputFormatProvider = new SalesforceInputFormatProvider(
      salesforceMultiSourceConfig, stringStringMap, salesforceSplitList, "S Object Name Field");
  }


  @Test
  public void testCInputFormatConfigurationSize() {
    HashMap<String, String> schemas = new HashMap<>();

    SalesforceInputFormatProvider salesforceInputFormatProvider = new SalesforceInputFormatProvider
      (salesforceMultiSourceConfig, schemas, new ArrayList(),
       "S Object Name Field");
    Assert.assertEquals(5, salesforceInputFormatProvider.getInputFormatConfiguration().size());
  }

  @Test
  public void testGetInputFormatClassName() {
    HashMap<String, String> schemas = new HashMap<>();
    Assert.assertEquals("io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat",
                        (new SalesforceInputFormatProvider(salesforceMultiSourceConfig, schemas, new ArrayList(),
                                                           "S Object Name Field"))
                          .getInputFormatClassName());
  }

}
