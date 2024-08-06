package io.cdap.plugin.servicenow.restapi;

import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  RestAPIClient.class,
  HttpClientBuilder.class,
  RestAPIResponse.class,
  ServiceNowTableAPIClientImpl.class,
  EntityUtils.class
})
public class RestAPIClientTest {

  @Test
  public void testExecuteGet_throwRetryableException() throws IOException {
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(BasicStatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(429);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    ServiceNowTableAPIRequestBuilder builder = new ServiceNowTableAPIRequestBuilder("url");
    RestAPIRequest request = builder.build();

    ServiceNowConnectorConfig config = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl client = new ServiceNowTableAPIClientImpl(config);
    RestAPIResponse actualResponse = client.executeGet(request);
    Assert.assertNotNull(actualResponse.getException());
    Assert.assertTrue(actualResponse.getException().isErrorRetryable());
  }

  @Test
  public void testExecuteGet_throwNonRetryableException() throws IOException {
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(BasicStatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    ServiceNowTableAPIRequestBuilder builder = new ServiceNowTableAPIRequestBuilder("url");
    RestAPIRequest request = builder.build();

    ServiceNowConnectorConfig config = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl client = new ServiceNowTableAPIClientImpl(config);
    RestAPIResponse actualResponse = client.executeGet(request);
    Assert.assertNotNull(actualResponse.getException());
    Assert.assertFalse(actualResponse.getException().isErrorRetryable());
  }

  @Test
  public void testExecuteGet_StatusOk() throws IOException {
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(BasicStatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    PowerMockito.mockStatic(EntityUtils.class);
    PowerMockito.when(EntityUtils.toString(Mockito.any())).thenReturn("{}");

    ServiceNowTableAPIRequestBuilder builder = new ServiceNowTableAPIRequestBuilder("url");
    RestAPIRequest request = builder.build();

    ServiceNowConnectorConfig config = Mockito.mock(ServiceNowConnectorConfig.class);
    ServiceNowTableAPIClientImpl client = new ServiceNowTableAPIClientImpl(config);
    client.executeGet(request);
  }
}
