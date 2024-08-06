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
package io.cdap.plugin.servicenow.sink;

import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.servicenow.ServiceNowBaseConfig;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.sink.transform.ServiceNowTransformer;
import io.cdap.plugin.servicenow.source.ServiceNowBaseSourceConfig;
import org.apache.hadoop.io.NullWritable;
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
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowTableAPIClientImpl.class, ServiceNowBaseSourceConfig.class, ServiceNowSink.class,
  HttpClientBuilder.class, RestAPIResponse.class})
public class ServiceNowSinkTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private ServiceNowSink serviceNowSink;
  private ServiceNowSinkConfig serviceNowSinkConfig;

  @Before
  public void initialize() {
    serviceNowSinkConfig = ServiceNowSinkConfigHelper.newConfigBuilder()
      .setReferenceName("referenceName")
      .setRestApiEndpoint(REST_API_ENDPOINT)
      .setUser(USER)
      .setPassword(PASSWORD)
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setTableName("sys_user")
      .setOperation("Insert")
      .build();
    serviceNowSink = new ServiceNowSink(serviceNowSinkConfig);
  }

  @Test
  public void testConfigurePipeline() throws Exception {
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
    MockFailureCollector collector = new MockFailureCollector();
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    serviceNowSink.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(restAPIResponse.getException());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRun() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
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
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    Emitter<KeyValue<NullWritable, JsonObject>> emitter = Mockito.mock(Emitter.class);
    Mockito.when(context.getInputSchema()).thenReturn(schema);
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
    BatchRuntimeContext batchRuntimeContext = Mockito.mock(BatchRuntimeContext.class);
    StructuredRecord record = Mockito.mock(StructuredRecord.class);
    Mockito.when(record.get("id")).thenReturn(1L);
    Mockito.when(record.get("price")).thenReturn(20.2008);
    Mockito.when(record.getSchema()).thenReturn(schema);
    ServiceNowTransformer recordToJsonTransformer = new ServiceNowTransformer();
    recordToJsonTransformer.transform(record);
    serviceNowSink.initialize(batchRuntimeContext);
    serviceNowSink.transform(record, emitter);
    serviceNowSink.prepareRun(context);
    Assert.assertEquals("1", record.get("id").toString());
    Assert.assertEquals("20.2008", record.get("price").toString());
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
}
