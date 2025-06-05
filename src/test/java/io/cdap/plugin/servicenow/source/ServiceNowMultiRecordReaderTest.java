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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableDataResponse;
import io.cdap.plugin.servicenow.connector.ServiceNowRecordConverter;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServiceNowMultiRecordReaderTest {

  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private ServiceNowMultiSourceConfig serviceNowMultiSourceConfig;
  private ServiceNowMultiRecordReader serviceNowMultiRecordReader;

  @Before
  public void initializeTests() {
    serviceNowMultiSourceConfig = Mockito.spy(new ServiceNowSourceConfigHelper.ConfigBuilder()
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
                                                .buildMultiSource());

    serviceNowMultiRecordReader = Mockito.spy(new ServiceNowMultiRecordReader(serviceNowMultiSourceConfig));
  }

  @Test
  public void testConstructor() throws IOException {
    Assert.assertEquals("tablename", serviceNowMultiSourceConfig.getTableNameField());
    Assert.assertEquals("user", serviceNowMultiSourceConfig.getConnection().getUser());
    Assert.assertEquals("sys_user", serviceNowMultiSourceConfig.getTableNames());
    Assert.assertEquals("2021-01-01", serviceNowMultiSourceConfig.getStartDate());
    Assert.assertEquals("https://ven05127.service-now.com", serviceNowMultiSourceConfig.getConnection()
      .getRestApiEndpoint());
    Assert.assertEquals("referenceName", serviceNowMultiSourceConfig.getReferenceName());
    Assert.assertEquals("2022-02-18", serviceNowMultiSourceConfig.getEndDate());
    PluginProperties properties = serviceNowMultiSourceConfig.getProperties();
    Assert.assertTrue(properties.getProperties().isEmpty());
    serviceNowMultiRecordReader.close();
    Assert.assertEquals(0, serviceNowMultiRecordReader.pos);
  }

  @Test(expected = IllegalStateException.class)
  public void testConvertToValueInvalidFieldType() {
    Schema fieldSchema = Schema.recordOf("record", Schema.Field.of("TimeField",
                                                                   Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(fieldSchema);
    Map<String, String> map = new HashMap<>();
    map.put("TimeField", "value");
    ServiceNowRecordConverter.convertToValue("TimeField", fieldSchema, map, recordBuilder);
  }

  @Test
  public void testConvertToDoubleValue() throws ParseException {
    Assert.assertEquals(42.0, ServiceNowRecordConverter.convertToDoubleValue("42"), 0.0);
  }

  @Test
  public void testConvertToIntegerValue() throws ParseException {
    Assert.assertEquals(42, ServiceNowRecordConverter.convertToIntegerValue("42").intValue());
  }

  @Test
  public void testConvertToBooleanValue() {
    Assert.assertTrue(ServiceNowRecordConverter.convertToBooleanValue("true"));
  }

  @Test(expected = UnexpectedFormatException.class)
  public void testConvertToBooleanValueForInvalidFieldValue() {
    Assert.assertTrue(ServiceNowRecordConverter.convertToBooleanValue("1"));
  }

  @Test
  public void testFetchData() throws ServiceNowAPIException, IOException {
    String tableName = serviceNowMultiSourceConfig.getTableNames();
    ServiceNowInputSplit split = new ServiceNowInputSplit(tableName, 1);

    List<Map<String, String>> results = new ArrayList<>();
    Map<String, String> map = new HashMap<>();
    map.put("calendar_integration", "1");
    map.put("country", "India");
    map.put("sys_updated_on", "2019-04-05 21:54:45");
    map.put("web_service_access_only", "false");
    map.put("notification", "2");
    map.put("enable_multifactor_authn", "false");
    map.put("sys_updated_by", "system");
    map.put("sys_created_on", "2019-04-05 21:09:12");
    results.add(map);

    ServiceNowTableDataResponse response = new ServiceNowTableDataResponse();
    response.setResult(results);
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    try {
      Mockito.when(restApi.fetchTableSchema(tableName, serviceNowMultiSourceConfig.getValueType()))
        .thenReturn(Schema.recordOf(Schema.Field.of("calendar_integration", Schema.of(Schema.Type.STRING))));
      serviceNowMultiRecordReader.initialize(split, null);
    } catch (RuntimeException
             | ServiceNowAPIException e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }
    Mockito.doNothing().when(serviceNowMultiRecordReader).fetchData();
    Collections.singletonList(new Object());
    serviceNowMultiRecordReader.iterator = Collections.singletonList(Collections.singletonMap("key", new String())).
      iterator();
    Assert.assertTrue(serviceNowMultiRecordReader.nextKeyValue());
  }

  @Test(expected = ServiceNowAPIException.class)
  public void testFetchDataOnInvalidTable()
      throws IOException, OAuthProblemException, OAuthSystemException, ServiceNowAPIException {
    serviceNowMultiSourceConfig =
        ServiceNowSourceConfigHelper.newConfigBuilder()
            .setReferenceName("referenceName")
            .setRestApiEndpoint(REST_API_ENDPOINT)
            .setUser(USER)
            .setPassword(PASSWORD)
            .setClientId(CLIENT_ID)
            .setClientSecret(CLIENT_SECRET)
            .setTableNames("")
            .setValueType("Actual")
            .setStartDate("2021-01-01")
            .setEndDate("2022-02-18")
            .setPageSize(10)
            .setTableNameField("tablename")
            .buildMultiSource();

    String tableName = serviceNowMultiSourceConfig.getTableNames();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    ServiceNowInputSplit split = new ServiceNowInputSplit(tableName, 1);
    List<Map<String, String>> results = new ArrayList<>();
    Map<String, String> map = new HashMap<>();
    map.put("calendar_integration", "1");
    map.put("country", "India");
    map.put("sys_updated_on", "2019-04-05 21:54:45");
    map.put("web_service_access_only", "false");
    map.put("notification", "2");
    map.put("enable_multifactor_authn", "false");
    map.put("sys_updated_by", "system");
    map.put("sys_created_on", "2019-04-05 21:09:12");
    results.add(map);
    restApi.fetchTableRecords(tableName, serviceNowMultiSourceConfig.getValueType(),
                              serviceNowMultiSourceConfig.getStartDate(), serviceNowMultiSourceConfig.getEndDate(),
                              split.getOffset(),
                              serviceNowMultiSourceConfig.getPageSize());

    ServiceNowTableDataResponse response = new ServiceNowTableDataResponse();
    response.setResult(results);
    try {
      Mockito.when(restApi.fetchTableSchema(tableName, serviceNowMultiSourceConfig.getValueType()))
        .thenReturn(Schema.recordOf(Schema.Field.of("calendar_integration", Schema.of(Schema.Type.STRING))));
      serviceNowMultiRecordReader.initialize(split, null);
    } catch (RuntimeException e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }
    serviceNowMultiRecordReader.fetchData();
  }
}
