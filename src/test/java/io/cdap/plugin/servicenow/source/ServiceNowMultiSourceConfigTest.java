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

import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import org.apache.http.HttpStatus;
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

/**
 * Tests for {@link ServiceNowMultiSourceConfig}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowTableAPIClientImpl.class, ServiceNowMultiSourceConfig.class})
public class ServiceNowMultiSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private ServiceNowMultiSourceConfig serviceNowMultiSourceConfig;

  @Test
  public void testConstructor() {
    serviceNowMultiSourceConfig = new ServiceNowMultiSourceConfig("referenceName", "client_id",
      "client_secret", "https://example.com", "user", "password",
      "tablename", "Actual", "2021-12-30", "2021-12-31", 10,
      "sys_user");
    Assert.assertEquals("sys_user", serviceNowMultiSourceConfig.getTableNames());
    Assert.assertEquals("Actual", serviceNowMultiSourceConfig.getValueType().getValueType());
    Assert.assertEquals("2021-12-30", serviceNowMultiSourceConfig.getStartDate());
    Assert.assertEquals("2021-12-31", serviceNowMultiSourceConfig.getEndDate());
    Assert.assertEquals("tablename", serviceNowMultiSourceConfig.getTableNameField());
  }

  @Test
  public void testValidateInvalidConnection() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();
    try {
      serviceNowMultiSourceConfig.validate(mockFailureCollector);
      Assert.fail("Exception is not thrown if connection is successful");
    } catch (ValidationException e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals("Unable to connect to ServiceNow Instance.",
                          mockFailureCollector.getValidationFailures().get(0).getMessage());
    }
  }

  @Test
  public void testValidate() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Map<String, String> headers = new HashMap<>();
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
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
    serviceNowMultiSourceConfig.validate(mockFailureCollector);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());

  }

  @Test
  public void testValidateWhenTableIsEmpty() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);

    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": []\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(headers, responseBody, null);
    Mockito.when(restApi.executeGetWithRetries(Mockito.any())).thenReturn(restAPIResponse);
    serviceNowMultiSourceConfig.validate(mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    Assert.assertEquals("Table: sys_user is empty.", mockFailureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateReferenceName() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("Reference Name")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Map<String, String> headers = new HashMap<>();
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
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
    try {
      serviceNowMultiSourceConfig.validate(mockFailureCollector);
      Assert.fail("Exception is not thrown with valid reference name");
    } catch (ValidationException e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals("referenceName", mockFailureCollector.getValidationFailures().get(0).getCauses()
        .get(0).getAttribute("stageConfig"));
    }

  }

  @Test
  public void testValidateWhenTableFieldNameIsEmpty() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("")
        .buildMultiSource();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowConnectorConfig.class)
      .withArguments(Mockito.any(ServiceNowConnectorConfig.class)).thenReturn(restApi);
    Map<String, String> headers = new HashMap<>();
    Map<String, String> map = new HashMap<>();
    List<Map<String, String>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
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
    serviceNowMultiSourceConfig.validate(mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    Assert.assertEquals("Table name field must be specified.",
                        mockFailureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateTableNames() {
    ServiceNowMultiSourceConfig serviceNowMultiSourceConfig = new ServiceNowMultiSourceConfig(
      "Reference Name", "42", "client_secret",
      "https://config.us-east-2.amazonaws.com", "User", "password",
      "tablename", "Actual", "2020-03-01", "2020-03-01", 10,
      ",");
    serviceNowMultiSourceConfig.validateTableNames(new MockFailureCollector("Stage Name"));
    Assert.assertEquals("42", serviceNowMultiSourceConfig.getConnection().getClientId());
    Assert.assertEquals("tablename", serviceNowMultiSourceConfig.tableNameField);
    Assert.assertEquals("User", serviceNowMultiSourceConfig.getConnection().getUser());
    Assert.assertEquals(",", serviceNowMultiSourceConfig.getTableNames());
    Assert.assertEquals("2020-03-01", serviceNowMultiSourceConfig.getStartDate());
    Assert.assertEquals("https://config.us-east-2.amazonaws.com", serviceNowMultiSourceConfig.getConnection()
      .getRestApiEndpoint());
    Assert.assertEquals("Reference Name", serviceNowMultiSourceConfig.getReferenceName());
    Assert.assertEquals("password", serviceNowMultiSourceConfig.getConnection().getPassword());
    Assert.assertEquals("client_secret", serviceNowMultiSourceConfig.getConnection().getClientSecret());
    Assert.assertEquals("2020-03-01", serviceNowMultiSourceConfig.getEndDate());
  }

  @Test
  public void testValidateTableNames2() {
    ServiceNowMultiSourceConfig serviceNowMultiSourceConfig = new ServiceNowMultiSourceConfig(
      "Reference Name", "Table Name Field", "42", "Client Secret",
      "https://config.us-east-2.amazonaws.com", "User", "password", "42",
      "2020-03-01", "2020-03-01", 10, "");
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig.validateTableNames(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(1, validationFailures.size());
    ValidationFailure getResult = validationFailures.get(0);
    List<ValidationFailure.Cause> causes = getResult.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("Table names must be specified.", getResult.getMessage());
    Assert.assertEquals("Stage Name", getResult.getCorrectiveAction());
    Assert.assertEquals("Table names must be specified. Stage Name", getResult.getFullMessage());
    Assert.assertEquals("tableNames", causes.get(0).getAttributes().get("stageConfig"));
  }

  @Test
  public void testValidateTableNamesWhenTableNamesAreEmpty() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();

    serviceNowMultiSourceConfig.validateTableNames(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(1, validationFailures.size());
    Assert.assertEquals("Table names must be specified.", validationFailures.get(0).getMessage());
  }
}
