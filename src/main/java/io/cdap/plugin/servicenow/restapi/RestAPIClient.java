/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.servicenow.restapi;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An abstract class to call Rest API.
 */
public abstract class RestAPIClient {
  private static final Logger LOG = LoggerFactory.getLogger(RestAPIClient.class);

  /**
   * Executes the Rest API request and returns the response.
   *
   * @param request the Rest API request
   * @return an instance of RestAPIResponse object.
   */
  public RestAPIResponse executeGet(RestAPIRequest request) throws IOException {
    HttpGet httpGet = new HttpGet(request.getUrl());
    request.getHeaders().entrySet().forEach(e -> httpGet.addHeader(e.getKey(), e.getValue()));

    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
        return RestAPIResponse.parse(httpResponse, request.getResponseHeaders());
      }
    }
  }

  /**
   * Executes the Rest API request and returns the response with retries.
   *
   * @param request the Rest API request.
   * @return an instance of RestAPIResponse object.
   * @throws ServiceNowAPIException
   */
  public RestAPIResponse executeGetWithRetries(RestAPIRequest request)
      throws ServiceNowAPIException {
    Callable<RestAPIResponse> callable = () -> executeGet(request);
    return handleExecution(getRetryer(), callable);
  }

  private RestAPIResponse handleExecution(
      Retryer<RestAPIResponse> retryer, Callable<RestAPIResponse> callable)
      throws ServiceNowAPIException {
    try {
      RestAPIResponse response = retryer.call(callable);
      // Execution is successful
      if (response.hasException()) {
        // Execution is successful and returned non retryable error
        throw response.getException();
      }
      return response;
    } catch (RetryException e) {
      // Execution successful, returned retryable error and retries exhausted
      Attempt<?> apiResponseAttempt = e.getLastFailedAttempt();
      if (apiResponseAttempt.hasException()) {
        // last attempt has execution failure
        throw new ServiceNowAPIException(apiResponseAttempt.getExceptionCause(), null);
      } else {
        // last execution attempt was successful but has an error response
        // if execution is successful, it's expected to have a exception in response object
        RestAPIResponse response = (RestAPIResponse) apiResponseAttempt.getResult();
        throw response.getException();
      }
    } catch (ExecutionException e) {
      // Execution failed with error
      throw new ServiceNowAPIException(e, null);
    }
  }

  private Retryer<RestAPIResponse> getRetryer() {
    return RetryerBuilder.<RestAPIResponse>newBuilder()
        .retryIfResult(
            restAPIResponse ->
                restAPIResponse.hasException() && restAPIResponse.getException().isErrorRetryable())
        .withWaitStrategy(
            WaitStrategies.exponentialWait(ServiceNowConstants.WAIT_TIME, TimeUnit.MILLISECONDS))
        .withStopStrategy(
            StopStrategies.stopAfterAttempt(ServiceNowConstants.MAX_NUMBER_OF_RETRY_ATTEMPTS))
        .build();
  }

  /**
   * Executes the Rest API request and returns the response.
   *
   * @param request the Rest API request
   * @return an instance of RestAPIResponse object.
   */
  public RestAPIResponse executePost(RestAPIRequest request) throws IOException {
    HttpPost httpPost = new HttpPost(request.getUrl());
    request.getHeaders().entrySet().forEach(e -> httpPost.addHeader(e.getKey(), e.getValue()));
    httpPost.setEntity(request.getEntity());

    // We're retrying all transport exceptions while executing the HTTP POST method and the generic transport
    // exceptions in HttpClient are represented by the standard java.io.IOException class
    // https://hc.apache.org/httpclient-legacy/exception-handling.html
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
        return RestAPIResponse.parse(httpResponse, request.getResponseHeaders());
      }
    }
  }
  /**
   * Generates access token and returns the same.
   *
   * @param restApiEndpoint The rest API endpoint for ServiceNow
   * @param clientId The Client Id for ServiceNow
   * @param clientSecret The Client Secret for ServiceNow
   * @param user the user id for ServiceNow
   * @param password The password for ServiceNow
   * @return The access token
   * @throws OAuthSystemException
   * @throws OAuthProblemException
   */
  protected String generateAccessToken(String restApiEndpoint, String clientId, String clientSecret, String user,
                                       String password) throws OAuthSystemException, OAuthProblemException {
    String token = "NO-VALUE";

    OAuthClient client = new OAuthClient(new URLConnectionClient());
    OAuthClientRequest request = OAuthClientRequest.tokenLocation(restApiEndpoint)
      .setGrantType(GrantType.PASSWORD)
      .setClientId(clientId)
      .setClientSecret(clientSecret)
      .setUsername(user)
      .setPassword(password)
      .buildBodyMessage();

    token = client.accessToken(request, OAuth.HttpMethod.POST, OAuthJSONAccessTokenResponse.class).getAccessToken();
    return token;
  }
}
