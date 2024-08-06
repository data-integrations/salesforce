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
package io.cdap.plugin.servicenow.connector;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.ServiceNowBaseSourceConfig;
import io.cdap.plugin.servicenow.source.ServiceNowInputFormat;
import io.cdap.plugin.servicenow.source.ServiceNowSource;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfigHelper;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
import io.cdap.plugin.servicenow.util.SourceQueryMode;
import io.cdap.plugin.servicenow.util.SourceValueType;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowTableAPIClientImpl.class, ServiceNowBaseSourceConfig.class, ServiceNowSource.class,
  HttpClientBuilder.class, RestAPIResponse.class, ServiceNowInputFormat.class})
public class ServiceNowConnectorTest {

  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private ServiceNowSource serviceNowSource;
  private ServiceNowSourceConfig serviceNowSourceConfig;

  @Before
  public void initialize() {
    serviceNowSourceConfig = ServiceNowSourceConfigHelper.newConfigBuilder()
      .setReferenceName("referenceName")
      .setRestApiEndpoint(REST_API_ENDPOINT)
      .setUser(USER)
      .setPassword(PASSWORD)
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setTableName("sys_user")
      .setValueType("Actual")
      .setStartDate("2021-01-01")
      .setEndDate("2022-02-18")
      .setTableNameField("tablename")
      .build();
    serviceNowSource = new ServiceNowSource(serviceNowSourceConfig);
  }

  @Test
  public void testTest() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    ServiceNowConnector serviceNowConnector = new ServiceNowConnector(serviceNowSourceConfig.getConnection());
    serviceNowConnector.test(context);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testTestWithInvalidToken() throws Exception {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    ServiceNowConnector serviceNowConnector = new ServiceNowConnector(serviceNowSourceConfig.getConnection());
    serviceNowConnector.test(context);
    Assert.assertEquals(1, context.getFailureCollector().getValidationFailures().size());
  }

  @Test
  public void testGenerateSpec() throws Exception {
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    RestAPIResponse response = Mockito.spy(restAPIResponse);
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.mockStatic(RestAPIResponse.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    ServiceNowConnector serviceNowConnector = new ServiceNowConnector(serviceNowSourceConfig.getConnection());
    ServiceNowTableInfo serviceNowTableInfo = new ServiceNowTableInfo("table", getPluginSchema(), 1);
    List<ServiceNowTableInfo> list = new ArrayList<>();
    list.add(serviceNowTableInfo);
    PowerMockito.mockStatic(ServiceNowInputFormat.class);
    SourceQueryMode mode = SourceQueryMode.TABLE;
    SourceValueType valueType = SourceValueType.SHOW_DISPLAY_VALUE;
    Mockito.when(ServiceNowInputFormat.fetchTableInfo(mode, serviceNowSourceConfig.getConnection(),
                                                      serviceNowSourceConfig.getTableName(),
                                                      null)).thenReturn(list);

    ConnectorSpec connectorSpec = serviceNowConnector.generateSpec(new MockConnectorContext
                                                                     (new MockConnectorConfigurer()),
                                                                   ConnectorSpecRequest.builder().setPath
                                                                       (serviceNowSourceConfig.getTableName())
                                                                     .setConnection("${conn(connection-id)}").build());

    Schema schema = connectorSpec.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(2, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(ServiceNowConstants.PLUGIN_NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"ServiceNowColumnMetaData\",\"fields\":[{\"name\":" +
      "\"backgroundElementId\",\"type\":\"long\"},{\"name\":\"bgOrderPos\",\"type\":\"long\"},{\"name\":" +
      "\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"userId\",\"type\":\"string\"}]}";
    return Schema.parseJson(schemaString);
  }
}
