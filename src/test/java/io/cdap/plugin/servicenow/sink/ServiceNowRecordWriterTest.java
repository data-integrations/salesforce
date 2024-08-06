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
import io.cdap.plugin.servicenow.ServiceNowBaseConfig;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIClient;
import io.cdap.plugin.servicenow.restapi.RestAPIRequest;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.sink.service.ServiceNowSinkAPIRequestImpl;
import io.cdap.plugin.servicenow.sink.transform.ServiceNowRecordWriter;
import io.cdap.plugin.servicenow.source.ServiceNowBaseSourceConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
@PrepareForTest({ServiceNowBaseSourceConfig.class,
  HttpClientBuilder.class, RestAPIResponse.class, ServiceNowTableAPIClientImpl.class, RestAPIClient.class,
  ServiceNowSinkAPIRequestImpl.class})
public class ServiceNowRecordWriterTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private ServiceNowSinkConfig serviceNowSinkConfig;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
      .setOperation("insert")
      .build();
  }

  @Test
  public void testWriteWithUnSuccessfulApiResponse() throws Exception {
    String responseBody = "{\"batch_request_id\":\"1\",\"serviced_requests\":[{\"id\":\"1\",\"body\":" +
      "\"eyJyZXN1bHQiOnsibnVtYmVyIjoiUkNTMDEwOTk3OCIsInN5c19pZCI6ImJkZGQwZjk2ODcwODE5MTA0YTkxNjUzODNjYmIzNTM0Iiwic3R" +
      "vY2tyb29tIjoiIiwic3lzX3VwZGF0ZWRfYnkiOiJwaXBlbGluZS51c2VyLjEiLCJwdXJjaGFzZV9vcmRlciI6IiIsInN5c19jcmVhdGVkX29" +
      "uIjoiMjAyMi0wNi0xNSAwNjozOTo1MyIsInN5c19kb21haW4iOnsibGluayI6Imh0dHBzOi8vdmVuMDUxMjcuc2VydmljZS1ub3cuY29tL2F" +
      "waS9ub3cvdGFibGUvc3lzX3VzZXJfZ3JvdXAvZ2xvYmFsIiwidmFsdWUiOiJnbG9iYWwifSwic3lzX21vZF9jb3VudCI6IjAiLCJyZWNlaXZl" +
      "ZCI6IjIwMjItMDYtMTUgMDY6Mzk6NTMiLCJzeXNfdXBkYXRlZF9vbiI6IjIwMjItMDYtMTUgMDY6Mzk6NTMiLCJzeXNfdGFncyI6IiIsInN" +
      "5c19jcmVhdGVkX2J5IjoicGlwZWxpbmUudXNlci4xIn19\",\"status_code\":201,\"status_text\":\"Created\",\"headers\":" +
      "[],\"execution_time\":8}]}";
    JsonObject jsonObject = new JsonObject();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    RestAPIResponse restAPIResponse = new RestAPIResponse(
        headers, responseBody, null);
    Mockito.when(restApi.executePost(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    ServiceNowRecordWriter serviceNowRecordWriter = new ServiceNowRecordWriter(serviceNowSinkConfig);
    serviceNowRecordWriter.write(null, jsonObject);
//    Assert.assertNotNull(restAPIResponse.getException());
//    Assert.assertFalse(restAPIResponse.getException().isErrorRetryable());
  }

  @Test
  public void testWriteWithSuccessFulApiResponse() throws Exception {
    String responseBody = "{\"batch_request_id\":\"1\",\"serviced_requests\":[{\"id\":\"1\",\"body\":\"eyJyZXN1bHQiO" +
      "nsibnVtYmVyIjoiUkNTMDEwOTk3OCIsInN5c19pZCI6ImJkZGQwZjk2ODcwODE5MTA0YTkxNjUzODNjYmIzNTM0Iiwic3RvY2tyb29tIjoiI" +
      "iwic3lzX3VwZGF0ZWRfYnkiOiJwaXBlbGluZS51c2VyLjEiLCJwdXJjaGFzZV9vcmRlciI6IiIsInN5c19jcmVhdGVkX29uIjoiMjAyMi0wN" +
      "i0xNSAwNjozOTo1MyIsInN5c19kb21haW4iOnsibGluayI6Imh0dHBzOi8vdmVuMDUxMjcuc2VydmljZS1ub3cuY29tL2FwaS9ub3cvdGFibGU" +
      "vc3lzX3VzZXJfZ3JvdXAvZ2xvYmFsIiwidmFsdWUiOiJnbG9iYWwifSwic3lzX21vZF9jb3VudCI6IjAiLCJyZWNlaXZlZCI6IjIwMjItMDYtM" +
      "TUgMDY6Mzk6NTMiLCJzeXNfdXBkYXRlZF9vbiI6IjIwMjItMDYtMTUgMDY6Mzk6NTMiLCJzeXNfdGFncyI6IiIsInN5c19jcmVhdGVkX2J5Ijo" +
      "icGlwZWxpbmUudXNlci4xIn19\",\"status_code\":201,\"status_text\":\"Created\",\"headers\":[],\"execution_time\"" +
      ":8}],\"unserviced_requests\":[]}";
    JsonObject jsonObject = new JsonObject();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    ServiceNowSinkAPIRequestImpl serviceNowSinkAPIRequest = Mockito.mock(ServiceNowSinkAPIRequestImpl.class);
    PowerMockito.whenNew(ServiceNowSinkAPIRequestImpl.class).withParameterTypes(ServiceNowSinkConfig.class)
      .withArguments(Mockito.any(ServiceNowSinkConfig.class)).thenReturn(serviceNowSinkAPIRequest);
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executePost(Mockito.any(RestAPIRequest.class))).thenReturn(restAPIResponse);
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
    ServiceNowRecordWriter serviceNowRecordWriter = new ServiceNowRecordWriter(serviceNowSinkConfig);
    serviceNowRecordWriter.write(null, jsonObject);
    Assert.assertNull(restAPIResponse.getException());
  }

  @Test
  public void testWriteWithUnservicedRequests() throws Exception {
    String responseBody = "{\"batch_request_id\":\"1\",\"serviced_requests\":[{\"id\":\"1\",\"body\":\"eyJyZXN1bHQiO" +
      "nsibnVtYmVyIjoiUkNTMDEwOTk3OCIsInN5c19pZCI6ImJkZGQwZjk2ODcwODE5MTA0YTkxNjUzODNjYmIzNTM0Iiwic3RvY2tyb29tIjoiI" +
      "iwic3lzX3VwZGF0ZWRfYnkiOiJwaXBlbGluZS51c2VyLjEiLCJwdXJjaGFzZV9vcmRlciI6IiIsInN5c19jcmVhdGVkX29uIjoiMjAyMi0wN" +
      "i0xNSAwNjozOTo1MyIsInN5c19kb21haW4iOnsibGluayI6Imh0dHBzOi8vdmVuMDUxMjcuc2VydmljZS1ub3cuY29tL2FwaS9ub3cvdGFibGU" +
      "vc3lzX3VzZXJfZ3JvdXAvZ2xvYmFsIiwidmFsdWUiOiJnbG9iYWwifSwic3lzX21vZF9jb3VudCI6IjAiLCJyZWNlaXZlZCI6IjIwMjItMDYtM" +
      "TUgMDY6Mzk6NTMiLCJzeXNfdXBkYXRlZF9vbiI6IjIwMjItMDYtMTUgMDY6Mzk6NTMiLCJzeXNfdGFncyI6IiIsInN5c19jcmVhdGVkX2J5Ijo" +
      "icGlwZWxpbmUudXNlci4xIn19\",\"status_code\":201,\"status_text\":\"Created\",\"headers\":[],\"execution_time\"" +
      ":8}],\"unserviced_requests\":[\"200\"]}";
    JsonObject jsonObject = new JsonObject();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    ServiceNowSinkAPIRequestImpl serviceNowSinkAPIRequest = Mockito.mock(ServiceNowSinkAPIRequestImpl.class);
    PowerMockito.whenNew(ServiceNowSinkAPIRequestImpl.class).withParameterTypes(ServiceNowSinkConfig.class)
      .withArguments(Mockito.any(ServiceNowSinkConfig.class)).thenReturn(serviceNowSinkAPIRequest);
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executePost(Mockito.any(RestAPIRequest.class))).thenReturn(restAPIResponse);
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
    ServiceNowRecordWriter serviceNowRecordWriter = new ServiceNowRecordWriter(serviceNowSinkConfig);
    serviceNowRecordWriter.write(null, jsonObject);
    Assert.assertNull(restAPIResponse.getException());
  }
}
