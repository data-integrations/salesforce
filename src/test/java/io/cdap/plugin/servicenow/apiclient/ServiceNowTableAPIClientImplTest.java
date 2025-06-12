package io.cdap.plugin.servicenow.apiclient;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.util.SourceValueType;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class ServiceNowTableAPIClientImplTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();
  @Test
  public void testFetchTableRecordsRetryableMode_RetriesAndSucceeds() throws ServiceNowAPIException {
    ServiceNowConnectorConfig mockConfig = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl impl = new ServiceNowTableAPIClientImpl(mockConfig);
    ServiceNowTableAPIClientImpl implSpy = Mockito.spy(impl);
    List<Map<String, String>> mockResults = new ArrayList<>();
    mockResults.add(new HashMap<String, String>() {{
      put("keyTest", "valueTest");
    }});
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(Mockito.mock(StatusLine.class));
    Mockito.when(mockResponse.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_REQUEST_TIMEOUT);
    Mockito.doThrow(new ServiceNowAPIException("Retryable Error", mockResponse))
            .doReturn(mockResults)
                .when(implSpy).fetchTableRecords(
            Mockito.anyString(),
            Mockito.any(),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyInt(),
            Mockito.anyInt());
    List<Map<String, String>> receivedResults =
        implSpy.fetchTableRecordsRetryableMode(
            "test", SourceValueType.SHOW_DISPLAY_VALUE, "", "", 0, 0);
    Mockito.verify(implSpy, Mockito.times(2)).fetchTableRecords(
        Mockito.anyString(),
        Mockito.any(),
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyInt(),
        Mockito.anyInt());
    Assert.assertEquals(receivedResults, mockResults);
  }

  @Test
  public void testFetchTableRecordsRetryableMode_nonRetryable()
      throws ServiceNowAPIException {
    ServiceNowConnectorConfig mockConfig = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl impl = new ServiceNowTableAPIClientImpl(mockConfig);
    ServiceNowTableAPIClientImpl implSpy = Mockito.spy(impl);
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(Mockito.mock(StatusLine.class));
    Mockito.when(mockResponse.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Mockito.doThrow(
        new ServiceNowAPIException("Non-retryable Error", mockResponse))
        .doReturn(new ArrayList<>())
        .when(implSpy).fetchTableRecords(
            Mockito.anyString(),
            Mockito.any(),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyInt(),
            Mockito.anyInt());
    exceptionRule.expect(ServiceNowAPIException.class);
    exceptionRule.expectMessage("Data Recovery failed for batch 0 to 0.");
    implSpy.fetchTableRecordsRetryableMode(
        "test", SourceValueType.SHOW_DISPLAY_VALUE, "", "", 0, 0);
  }

  @Test
  public void testFetchTableSchema_ActualValueType() throws Exception {
    ServiceNowConnectorConfig mockConfig = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl impl = new ServiceNowTableAPIClientImpl(mockConfig);
    ServiceNowTableAPIClientImpl implSpy = Mockito.spy(impl);
    String jsonResponse = "{\n" +
      "  \"result\": {\n" +
      "    \"columns\": {\n" +
      "      \"active\": {\n" +
      "        \"label\": \"Active\",\n" +
      "        \"name\": \"active\",\n" +
      "        \"type\": \"string\",\n" +
      "        \"internal_type\": \"boolean\"\n" +
      "      },\n" +
      "      \"user_name\": {\n" +
      "        \"label\": \"Username\",\n" +
      "        \"name\": \"user_name\",\n" +
      "        \"type\": \"string\",\n" +
      "        \"internal_type\": \"string\"\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}";
    RestAPIResponse mockResponse = new RestAPIResponse(Collections.emptyMap(), jsonResponse, null);
    Mockito.doReturn(mockResponse).when(implSpy).executeGetWithRetries(Mockito.any());
    Schema schema = implSpy.fetchTableSchema("sys_user", "dummy-access-token", SourceValueType.SHOW_ACTUAL_VALUE);
    Assert.assertNotNull(schema);
    Assert.assertEquals("record", schema.getDisplayName());
    Assert.assertEquals(2, schema.getFields().size());
    Assert.assertEquals(Schema.Type.BOOLEAN,
      schema.getField("active").getSchema().getUnionSchemas().get(0).getType());
    Assert.assertEquals(Schema.Type.STRING,
      schema.getField("user_name").getSchema().getUnionSchemas().get(0).getType());
  }

  @Test
  public void testFetchTableSchema_DisplayValueType() throws Exception {
    ServiceNowConnectorConfig mockConfig = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl impl = new ServiceNowTableAPIClientImpl(mockConfig);
    ServiceNowTableAPIClientImpl implSpy = Mockito.spy(impl);
    String jsonResponse = "{\n" +
      "  \"result\": {\n" +
      "    \"columns\": {\n" +
      "      \"active\": {\n" +
      "        \"label\": \"Active\",\n" +
      "        \"name\": \"active\",\n" +
      "        \"type\": \"string\",\n" +
      "        \"internal_type\": \"boolean\"\n" +
      "      },\n" +
      "      \"user_name\": {\n" +
      "        \"label\": \"Username\",\n" +
      "        \"name\": \"user_name\",\n" +
      "        \"type\": \"string\",\n" +
      "        \"internal_type\": \"string\"\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}";
    RestAPIResponse mockResponse = new RestAPIResponse(Collections.emptyMap(), jsonResponse, null);
    Mockito.doReturn(mockResponse).when(implSpy).executeGetWithRetries(Mockito.any());
    Schema schema = implSpy.fetchTableSchema("sys_user", "dummy-access-token", SourceValueType.SHOW_DISPLAY_VALUE);
    Assert.assertNotNull(schema);
    Assert.assertEquals("record", schema.getDisplayName());
    Assert.assertEquals(2, schema.getFields().size());
    Assert.assertEquals(Schema.Type.STRING,
      schema.getField("active").getSchema().getUnionSchemas().get(0).getType());
    Assert.assertEquals(Schema.Type.STRING,
      schema.getField("user_name").getSchema().getUnionSchemas().get(0).getType());
  }
}
