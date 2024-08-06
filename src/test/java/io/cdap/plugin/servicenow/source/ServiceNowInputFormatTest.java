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


package io.cdap.plugin.servicenow.source;

import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.util.SourceApplication;
import io.cdap.plugin.servicenow.util.SourceQueryMode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowTableAPIClientImpl.class, ServiceNowBaseSourceConfig.class, ServiceNowMultiSource.class,
  HttpClientBuilder.class, RestAPIResponse.class, ServiceNowInputFormat.class})
public class ServiceNowInputFormatTest {

  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowInputFormat.class);
  private ServiceNowConnectorConfig connectorConfig;

  @Before
  public void initializeTests() {
    connectorConfig = Mockito.spy(new ServiceNowConnectorConfig(CLIENT_ID, CLIENT_SECRET, REST_API_ENDPOINT, USER,
                                                                PASSWORD));
  }

  @Test
  public void testFetchTableInfo() throws Exception {
    SourceQueryMode mode = SourceQueryMode.TABLE;
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    List<Map<String, String>> result = new ArrayList<>();
    Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"country\": \"\",\n" +
      "            \"last_login_time\": \"2019-04-05 22:16:30\",\n" +
      "            \"source\": \"\",\n" +
      "            \"sys_updated_on\": \"2019-04-05 21:54:45\",\n" +
      "            \"building\": \"\",\n" +
      "            \"web_service_access_only\": \"false\",\n" +
      "            \"notification\": \"2\",\n" +
      "            \"enable_multifactor_authn\": \"false\",\n" +
      "            \"sys_updated_by\": \"system\",\n" +
      "            \"sys_created_on\": \"2019-04-05 21:09:12\",\n" +
      "            \"sys_domain\": {\n" +
      "                \"link\": \"https://ven05127.service-now.com/api/now/table/sys_user_group/global\",\n" +
      "                \"value\": \"global\"\n" +
      "            },\n" +
      "            \"state\": \"\",\n" +
      "            \"vip\": \"false\",\n" +
      "            \"sys_created_by\": \"admin\",\n" +
      "            \"zip\": \"\",\n" +
      "            \"home_phone\": \"\",\n" +
      "            \"time_format\": \"\",\n" +
      "            \"last_login\": \"2019-04-05\",\n" +
      "            \"active\": \"true\",\n" +
      "            \"sys_domain_path\": \"/\",\n" +
      "            \"cost_center\": \"\",\n" +
      "            \"phone\": \"\",\n" +
      "            \"name\": \"survey user\",\n" +
      "            \"employee_number\": \"\",\n" +
      "            \"gender\": \"\",\n" +
      "            \"city\": \"\",\n" +
      "            \"failed_attempts\": \"0\",\n" +
      "            \"user_name\": \"survey.user\",\n" +
      "            \"title\": \"\",\n" +
      "            \"sys_class_name\": \"sys_user\",\n" +
      "            \"sys_id\": \"005d500b536073005e0addeeff7b12f4\",\n" +
      "            \"internal_integration_user\": \"false\",\n" +
      "            \"ldap_server\": \"\",\n" +
      "            \"mobile_phone\": \"\",\n" +
      "            \"street\": \"\",\n" +
      "            \"company\": \"\",\n" +
      "            \"department\": \"\",\n" +
      "            \"first_name\": \"survey\",\n" +
      "            \"email\": \"survey.user@email.com\",\n" +
      "            \"introduction\": \"\",\n" +
      "            \"preferred_language\": \"\",\n" +
      "            \"manager\": \"\",\n" +
      "            \"sys_mod_count\": \"1\",\n" +
      "            \"last_name\": \"user\",\n" +
      "            \"photo\": \"\",\n" +
      "            \"avatar\": \"\",\n" +
      "            \"middle_name\": \"\",\n" +
      "            \"sys_tags\": \"\",\n" +
      "            \"time_zone\": \"\",\n" +
      "            \"schedule\": \"\",\n" +
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
    SourceApplication application = SourceApplication.PROCUREMENT;
    Assert.assertEquals(1, ServiceNowInputFormat.fetchTableInfo(mode, connectorConfig, "table",
                                                                application).size());
  }

  @Test
  public void testFetchTableInfoReportingMode() throws Exception {
    SourceQueryMode mode = SourceQueryMode.REPORTING;
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    List<Map<String, String>> result = new ArrayList<>();
    Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"country\": \"\",\n" +
      "            \"last_login_time\": \"2019-04-05 22:16:30\",\n" +
      "            \"source\": \"\",\n" +
      "            \"sys_updated_on\": \"2019-04-05 21:54:45\",\n" +
      "            \"building\": \"\",\n" +
      "            \"web_service_access_only\": \"false\",\n" +
      "            \"notification\": \"2\",\n" +
      "            \"enable_multifactor_authn\": \"false\",\n" +
      "            \"sys_updated_by\": \"system\",\n" +
      "            \"sys_created_on\": \"2019-04-05 21:09:12\",\n" +
      "            \"sys_domain\": {\n" +
      "                \"link\": \"https://ven05127.service-now.com/api/now/table/sys_user_group/global\",\n" +
      "                \"value\": \"global\"\n" +
      "            },\n" +
      "            \"state\": \"\",\n" +
      "            \"vip\": \"false\",\n" +
      "            \"sys_created_by\": \"admin\",\n" +
      "            \"zip\": \"\",\n" +
      "            \"home_phone\": \"\",\n" +
      "            \"time_format\": \"\",\n" +
      "            \"last_login\": \"2019-04-05\",\n" +
      "            \"active\": \"true\",\n" +
      "            \"sys_domain_path\": \"/\",\n" +
      "            \"cost_center\": \"\",\n" +
      "            \"phone\": \"\",\n" +
      "            \"name\": \"survey user\",\n" +
      "            \"employee_number\": \"\",\n" +
      "            \"gender\": \"\",\n" +
      "            \"city\": \"\",\n" +
      "            \"failed_attempts\": \"0\",\n" +
      "            \"user_name\": \"survey.user\",\n" +
      "            \"title\": \"\",\n" +
      "            \"sys_class_name\": \"sys_user\",\n" +
      "            \"sys_id\": \"005d500b536073005e0addeeff7b12f4\",\n" +
      "            \"internal_integration_user\": \"false\",\n" +
      "            \"ldap_server\": \"\",\n" +
      "            \"mobile_phone\": \"\",\n" +
      "            \"street\": \"\",\n" +
      "            \"company\": \"\",\n" +
      "            \"department\": \"\",\n" +
      "            \"first_name\": \"survey\",\n" +
      "            \"email\": \"survey.user@email.com\",\n" +
      "            \"introduction\": \"\",\n" +
      "            \"preferred_language\": \"\",\n" +
      "            \"manager\": \"\",\n" +
      "            \"sys_mod_count\": \"1\",\n" +
      "            \"last_name\": \"user\",\n" +
      "            \"photo\": \"\",\n" +
      "            \"avatar\": \"\",\n" +
      "            \"middle_name\": \"\",\n" +
      "            \"sys_tags\": \"\",\n" +
      "            \"time_zone\": \"\",\n" +
      "            \"schedule\": \"\",\n" +
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
    SourceApplication application = SourceApplication.PROCUREMENT;
    Assert.assertEquals(4, ServiceNowInputFormat.fetchTableInfo(mode, connectorConfig, "table",
                                                                application).size());
  }

  @Test
  public void testFetchTableInfoWithEmptyTableName() throws Exception {
    SourceQueryMode mode = SourceQueryMode.TABLE;
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    List<Map<String, String>> result = new ArrayList<>();
    Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": []\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.mockStatic(ServiceNowInputFormat.class);
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
    SourceApplication application = SourceApplication.PROCUREMENT;
    Assert.assertTrue(ServiceNowInputFormat.fetchTableInfo(mode, connectorConfig, "table",
                                                           application).isEmpty());
  }
}
