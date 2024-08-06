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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowTableAPIClientImpl.class, ServiceNowBaseSourceConfig.class, ServiceNowMultiSource.class,
  HttpClientBuilder.class, RestAPIResponse.class, ServiceNowInputFormat.class, ServiceNowMultiInputFormat.class})
public class ServiceNowMultiSourceTest {

  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private ServiceNowMultiSource serviceNowMultiSource;
  private ServiceNowMultiSourceConfig serviceNowMultiSourceConfig;

  @Before
  public void initialize() {
    serviceNowMultiSourceConfig = ServiceNowSourceConfigHelper.newConfigBuilder()
      .setReferenceName("referenceName")
      .setRestApiEndpoint(REST_API_ENDPOINT)
      .setUser(USER)
      .setPassword(PASSWORD)
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setTableNames("sys_user")
      .setValueType("Actual")
      .setStartDate("2021-01-01")
      .setEndDate("2022-02-18")
      .setTableNameField("tablename")
      .buildMultiSource();
    serviceNowMultiSource = new ServiceNowMultiSource(serviceNowMultiSourceConfig);
  }

  @Test
  public void testConfigurePipeline() throws Exception {
    Map<String, Object> plugins = new HashMap<>();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
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
    serviceNowMultiSource.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(mockPipelineConfigurer.getOutputSchema());
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConfigurePipelineWithEmptyTable() throws Exception {
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    List<Map<String, String>> result = new ArrayList<>();
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": []\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    try {
      serviceNowMultiSource.configurePipeline(mockPipelineConfigurer);
      Assert.fail("Exception is not thrown for Non-Empty Tables");
    } catch (ValidationException e) {
      Assert.assertEquals("Table: sys_user is empty.", e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testPrepareRun() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Set<ServiceNowTableInfo> tableInfo = new HashSet<>();
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("IntField", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("LongField", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("DoubleField", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("BooleanField", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("DateField", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("TimestampField",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("TimeField", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("StringField", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("ArrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    ServiceNowTableInfo serviceNowTableInfo = new ServiceNowTableInfo("table", schema, 1);
    tableInfo.add(serviceNowTableInfo);
    MockArguments mockArguments = new MockArguments();
    mockArguments.set("multisink.sys_user", "multisink." + serviceNowMultiSourceConfig.getTableNames());
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(mockArguments);
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
    PowerMockito.mockStatic(ServiceNowMultiInputFormat.class);
    Mockito.when(ServiceNowMultiInputFormat.setInput(Mockito.any(), Mockito.any())).thenReturn((tableInfo));
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
    PowerMockito.mockStatic(ServiceNowInputFormat.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    serviceNowMultiSource.prepareRun(context);
    Assert.assertNotNull(context.getArguments().get("multisink." + serviceNowMultiSourceConfig.getTableNames()));
    Assert.assertEquals("Mismatch in expected vs actual number of validation failures.", 0,
                        mockFailureCollector.getValidationFailures().size());
  }
}
