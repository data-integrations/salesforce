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
package io.cdap.plugin.servicenow.sink.service;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.sink.ServiceNowSinkConfig;
import io.cdap.plugin.servicenow.sink.model.RestRequest;
import io.cdap.plugin.servicenow.sink.model.ServiceNowBatchRequest;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MediaType;

/**
 * Implementation class for ServiceNow Batch Rest API.
 */
public class ServiceNowSinkAPIRequestImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSinkAPIRequestImpl.class);

  private final ServiceNowSinkConfig config;
  private final ServiceNowTableAPIClientImpl restApi;
  private final Gson gson = new Gson();
  private final JsonParser jsonParser = new JsonParser();

  public ServiceNowSinkAPIRequestImpl(ServiceNowSinkConfig conf) {
    this.config = conf;
    restApi = new ServiceNowTableAPIClientImpl(config.getConnection());
  }

  public RestRequest getRestRequest(JsonObject jsonObject) {
    JsonElement jsonElement = jsonObject.get(ServiceNowConstants.SYS_ID);
    String sysId = null;
    // sys_id is mandatory for update operation
    if (jsonElement == null && config.getOperation().equals(ServiceNowConstants.UPDATE_OPERATION)) {
      throw new IllegalStateException("No sys_id found in the record to be updated");
      // sys_id need to be removed for insert operation
    } else if (jsonElement != null && config.getOperation().equals(ServiceNowConstants.INSERT_OPERATION)) {
      jsonObject.remove(ServiceNowConstants.SYS_ID);
      // this condition will run when jsonElement is not null and operation is update
    } else if (jsonElement != null) {
      sysId = jsonElement.getAsString();
    }
    String data = jsonObject.toString();
    String encodedData = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));

    RestRequest restRequest = new RestRequest();
    restRequest.setUrl(getUrl(sysId));
    restRequest.setId(UUID.randomUUID().toString());
    restRequest.setHeaders(getHeaders());
    restRequest.setMethod(ServiceNowConstants.HTTP_POST);
    if (config.getOperation().equals(ServiceNowConstants.UPDATE_OPERATION)) {
      restRequest.setMethod(ServiceNowConstants.HTTP_PUT);
    }
    restRequest.setBody(encodedData);
    return restRequest;
  }

  /**
   * Inserts/Updates the list of records into ServiceNow table
   *
   * @param restRequestsMap The map of restRequests
   * @param accessToken    The access token
   */
  public void createPostRequest(Map<String, RestRequest> restRequestsMap, String accessToken)
      throws ServiceNowAPIException {
    ServiceNowBatchRequest payloadRequest = getPayloadRequest(restRequestsMap);
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getConnection().getRestApiEndpoint());
    RestAPIResponse apiResponse;

    try {
      requestBuilder.setAuthHeader(accessToken);
      requestBuilder.setAcceptHeader(MediaType.APPLICATION_JSON);
      requestBuilder.setContentTypeHeader(MediaType.APPLICATION_JSON);
      StringEntity stringEntity = new StringEntity(gson.toJson(payloadRequest), ContentType.APPLICATION_JSON);
      requestBuilder.setEntity(stringEntity);
      apiResponse = restApi.executePost(requestBuilder.build());

      JsonObject responseJSON = jsonParser.parse(apiResponse.getResponseBody()).getAsJsonObject();
      JsonArray servicedRequestsArray = responseJSON.get(ServiceNowConstants.SERVICED_REQUESTS).getAsJsonArray();
      JsonElement failedRequestId = null;
      for (int i = 0; i < servicedRequestsArray.size(); i++) {
        int statusCode = servicedRequestsArray.get(i).getAsJsonObject().get(ServiceNowConstants.STATUS_CODE)
                .getAsInt();
        if (statusCode / 100 == 4 || statusCode / 100 == 5) {
          String encodedResponseBody = servicedRequestsArray.get(i).getAsJsonObject().get(ServiceNowConstants.BODY)
                  .getAsString();
          String decodedResponseBody = new String(Base64.getDecoder().decode(encodedResponseBody));
          String errorDetail = jsonParser.parse(decodedResponseBody).getAsJsonObject().get(ServiceNowConstants.ERROR)
                  .getAsJsonObject().get(ServiceNowConstants.ERROR_DETAIL).getAsString();

          if (errorDetail.equals(ServiceNowConstants.ACL_EXCEPTION)) {
            throw new IllegalStateException(String.format("Permission denied for '%s' operation.",
                    config.getOperation()));
          } else if (errorDetail.contains(ServiceNowConstants.INSERT_ERROR) ||
                  errorDetail.equals(ServiceNowConstants.UPDATE_ERROR)) {
            LOG.warn("Error Response : {} ", decodedResponseBody);
          } else if (errorDetail.contains((ServiceNowConstants.MAXIMUM_EXECUTION_TIME_EXCEEDED))) {
            failedRequestId = servicedRequestsArray.get(i).getAsJsonObject().get(ServiceNowConstants.ID);
          } else {
            throw new IllegalStateException(errorDetail);
          }
        }
      }

      JsonArray unservicedRequestsArray = responseJSON.get(ServiceNowConstants.UNSERVICED_REQUESTS).getAsJsonArray();

      if (unservicedRequestsArray.size() > 0) {
        // Add failed request Id to unserviced requests
        if (failedRequestId != null) {
          unservicedRequestsArray.add(failedRequestId);
        }

        // Process unserviced requests array into unserviced requests map
        Map<String, RestRequest> unservicedRequestsMap = processUnservicedRequestsArray(restRequestsMap,
                unservicedRequestsArray);
        // Retry unserviced requests
        createPostRequest(unservicedRequestsMap, accessToken);
      }
    } catch (IOException e) {
      LOG.error("Error while connecting to ServiceNow", e.getMessage());
      throw new ServiceNowAPIException("Error while connecting to ServiceNow", e, null, true);
    }
  }

  private ServiceNowBatchRequest getPayloadRequest(Map<String, RestRequest> restRequests) {
    ServiceNowBatchRequest payloadRequest = new ServiceNowBatchRequest();
    payloadRequest.setBatchRequestId(UUID.randomUUID().toString());
    payloadRequest.setRestRequests(new ArrayList<>(restRequests.values()));
    return payloadRequest;
  }

  /**
   * Process the unserviced requests array into an unserviced requests map.
   *
   * @param restRequestsMap         The map of rest requests
   * @param unservicedRequestsArray The json array of unserviced request ids
   * @return unservicedRequestsMap The map of unserviced requests
   */
  private Map<String, RestRequest> processUnservicedRequestsArray(Map<String, RestRequest> restRequestsMap,
                                                                  JsonArray unservicedRequestsArray) {
    Map<String, RestRequest> unservicedRequestsMap = new HashMap<>();
    for (int i = 0; i < unservicedRequestsArray.size(); i++) {
      String requestId = unservicedRequestsArray.get(i).getAsString();
      RestRequest request = restRequestsMap.get(requestId);
      if (request == null) {
        LOG.error("No Rest Request found for Request Id {}", requestId);
        continue;
      }
      unservicedRequestsMap.put(requestId, request);
    }
    return unservicedRequestsMap;
  }

  /**
   * Retries to insert/update the records into ServiceNow table when RetryableException is thrown          .
   *
   * @param restRequestsMap The map of rest Requests
   */
  public void createPostRequestRetryableMode(Map<String, RestRequest> restRequestsMap) throws ExecutionException,
    RetryException {
    String accessToken = restApi.getAccessTokenRetryableMode();
    Callable<Boolean> fetchRecords = () -> {
      createPostRequest(restRequestsMap, accessToken);
      return true;
    };

    Retryer retryer = RetryerBuilder.newBuilder()
      .retryIfExceptionOfType(RetryableException.class)
      .withWaitStrategy(WaitStrategies.fixedWait(ServiceNowConstants.BASE_DELAY, TimeUnit.MILLISECONDS))
      .withStopStrategy(StopStrategies.stopAfterAttempt(ServiceNowConstants.MAX_NUMBER_OF_RETRY_ATTEMPTS))
      .build();

    retryer.call(fetchRecords);
  }

  /**
   * Gets the URL for ServiceNow Batch Request
   *
   * @param sysId
   * @return The URL String
   */
  private String getUrl(String sysId) {
    return config.getOperation().equals(ServiceNowConstants.UPDATE_OPERATION) ?
      String.format(ServiceNowConstants.UPDATE_TABLE_API_URL_TEMPLATE, config.getTableName(), sysId) :
      String.format(ServiceNowConstants.INSERT_TABLE_API_URL_TEMPLATE, config.getTableName());
  }

  /**
   * Gets the list of headers for ServiceNow Batch Request
   *
   * @return The list of headers
   */
  private List<Header> getHeaders() {
    List<Header> headers = new ArrayList<>();
    Header contentTypeHeader = new BasicHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    Header acceptHeader = new BasicHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
    headers.add(contentTypeHeader);
    headers.add(acceptHeader);
    return headers;
  }

}
