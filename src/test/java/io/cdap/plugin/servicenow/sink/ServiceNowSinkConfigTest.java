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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIClient;
import io.cdap.plugin.servicenow.restapi.RestAPIRequest;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.sink.model.SchemaResponse;
import io.cdap.plugin.servicenow.sink.model.ServiceNowSchemaField;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicStatusLine;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RestAPIClient.class, HttpClientBuilder.class, RestAPIResponse.class,
  ServiceNowTableAPIClientImpl.class})
public class ServiceNowSinkConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValidateClientIdNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientId(null).build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid client id");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_CLIENT_ID, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateClientSecretNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientSecret(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid client secret");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_CLIENT_SECRET, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateApiEndpointNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setRestApiEndpoint(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid Api Endpoint");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_API_ENDPOINT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateUserNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setUser(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid User");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_USER, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidatePasswordNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setPassword(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid password");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_PASSWORD, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidTableName() {
    ServiceNowSinkConfig config = ServiceNowSinkConfigHelper.newConfigBuilder()
      .setTableName("Table").setOperation(null).build();
    Assert.assertEquals("Table", config.getTableName());
  }

  @Test
  public void testEmptyTableName() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setTableName("")
                                                                 .build(), mockFailureCollector);
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    ValidationFailure getResult = validationFailures.get(0);
    Assert.assertEquals("Table name must be specified.", getResult.getMessage());
  }

  @Test
  public void testOperation() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder().
                                                                 setOperation("Insert").
                                                                 build(), collector);
    Assert.assertEquals("Insert", config.getOperation());
  }

  @Test
  public void testCheckCompatibilitySuccess() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema.
                                                                                                     LogicalType.DATE)))
      ,
                                          Schema.Field.of("ExtraField", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema
                                                                                                       .LogicalType.
                                                                                                       DATE))),
                                            Schema.Field.of("Comment", Schema.nullableOf(Schema.of
                                              (Schema.Type.STRING))));
    config.checkCompatibility(actualSchema, providedSchema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testCheckCompatibilityMissingField() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));
    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));
    try {
      config.checkCompatibility(actualSchema, providedSchema, collector);
      Assert.fail("No exception is to be thrown if missing field would be there.");
    } catch (ValidationException e) {
      Assert.assertEquals(1, collector.getValidationFailures().size());
    }
  }

  @Test
  public void testCheckCompatibilityWType() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));

    config.getSchema(collector);
    config.checkCompatibility(actualSchema, providedSchema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testCheckCompatibilityWithLogicalType() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.
                                                                                         TIMESTAMP_MICROS)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.
                                                                                           DATETIME)));
    try {
      config.checkCompatibility(actualSchema, providedSchema, collector);
      Assert.fail("No exception will be thrown if logical type will be same.");
    } catch (ValidationException e) {
      Assert.assertEquals(1, collector.getValidationFailures().size());
    }
  }

  @Test
  public void testCheckCompatibility() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));
    config.checkCompatibility(actualSchema, providedSchema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateSchema() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("operation")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withArguments(Mockito.any(ServiceNowSinkConfig.class))
      .thenReturn(restApi);
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_NOT_FOUND;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "  \"status_code\":201 " +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"country\": \"\",\n" +
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    ServiceNowSchemaField schemaField = new ServiceNowSchemaField("Class", "sys_class_name",
                                                                  "sys_class_name", "sys_class_name");
    List<ServiceNowSchemaField> schemaFields = new ArrayList<>();
    schemaFields.add(schemaField);
    SchemaResponse schemaResponse = new SchemaResponse(schemaFields);
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(Mockito.mock(StatusLine.class));
    Mockito.when(mockResponse.getStatusLine().getStatusCode()).thenReturn(httpStatus);
    RestAPIResponse restAPIResponse = new RestAPIResponse(
        headers, responseBody, new ServiceNowAPIException("", mockResponse));
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
    PowerMockito.when(RestAPIResponse.parse(httpResponse, null)).thenReturn(response);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any(RestAPIRequest.class))).thenReturn(restAPIResponse);
    Mockito.when(restApi.fetchTableSchema(Mockito.anyString(), Mockito.any(FailureCollector.class))).thenReturn(schema);
    Mockito.when(restApi.parseSchemaResponse(restAPIResponse.getResponseBody()))
      .thenReturn(schemaResponse);
    try {
      config.validateSchema(schema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown if apiResponse is successful");
    } catch (RuntimeException e) {
      //Exception as the apiResponse is unsuccessful, it will not be able to fetch the schema of the table.
      Assert.assertEquals(1, collector.getValidationFailures().size());
    }
  }

  @Test
  public void testValidateSchemaWithOperation() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("insert")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("sys_class_name", Schema.of(Schema.Type.STRING)));
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"label\": \"Class\",\n" +
      "            \"internalType\": \"sys_class_name\",\n" +
      "            \"exampleValue\": \"\",\n" +
      "            \"name\": \"sys_class_name\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
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
    StatusLine statusLine = Mockito.mock(BasicStatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(httpResponse, null)).thenReturn(response);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any(RestAPIRequest.class))).thenReturn(restAPIResponse);
    Mockito.when(restApi.fetchTableSchema("tableName", collector)).
      thenReturn(schema);
    config.validateSchema(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetSchemaNullFields() throws Exception {

    Schema schema = Schema.of(Schema.LogicalType.DATE);
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("insert")
                                                                             .build(), collector));
    try {
      config.validateSchema(schema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown if fields are provided");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation. Sink schema must contain at " +
                            "least one field", e.getMessage());
    }
  }

  private ServiceNowSinkConfig withServiceNowValidationMock(ServiceNowSinkConfig config,
                                                            FailureCollector collector) {
    ServiceNowSinkConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateServiceNowConnection(collector);
    return spy;
  }
}
