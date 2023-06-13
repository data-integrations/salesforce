/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents Salesforce Client.
 */
public class SalesforceClient {
  private static final Logger logger = LoggerFactory.getLogger(SalesforceClient.class);
  private static final String USERNAME = System.getenv("SALESFORCE_USERNAME");
  private static final String PASSWORD = System.getenv("SALESFORCE_PASSWORD");
  private static final String TOKEN_URL = PluginPropertyUtils.pluginProp("login.url");
  private static final String GRANTTYPE = PluginPropertyUtils.pluginProp("grant.type");
  private static final String CLIENTID = System.getenv("SALESFORCE_CONSUMER_KEY");
  private static final String SECURITYTOKEN = System.getenv("SALESFORCE_SECURITY_TOKEN");
  private static final String CLIENTSECRET = System.getenv("SALESFORCE_CONSUMER_SECRET");
  private static final String REST_ENDPOINT = PluginPropertyUtils.pluginProp("rest.api.endpoint");
  private static final String API_VERSION = PluginPropertyUtils.pluginProp("rest.api.version");
  private static final Header prettyPrintHeader = new BasicHeader("X-PrettyPrint", "1");
  public static List<JsonObject> sobjectResponse = new ArrayList<>();
  public static String uniqueRecordId;
  private static String loginAccessToken = null;
  private static String loginInstanceUrl = null;

  public static String getAccessToken() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    final List<NameValuePair> loginParams = new ArrayList<>();
    loginParams.add(new BasicNameValuePair("client_id", CLIENTID));
    loginParams.add(new BasicNameValuePair("client_secret", CLIENTSECRET));
    loginParams.add(new BasicNameValuePair("grant_type", GRANTTYPE));
    loginParams.add(new BasicNameValuePair("username", USERNAME));
    loginParams.add(new BasicNameValuePair("password", PASSWORD + SECURITYTOKEN));

    try {
      HttpPost httpPost = new HttpPost(TOKEN_URL);
      httpPost.setEntity(new UrlEncodedFormEntity(loginParams));
      HttpResponse response = httpClient.execute(httpPost);
      final int statusCode = response.getStatusLine().getStatusCode();
      String responseDetails = EntityUtils.toString(response.getEntity());
      JSONObject jsonObjectOfResponse = (JSONObject) new JSONTokener(responseDetails).nextValue();
      loginAccessToken = jsonObjectOfResponse.getString("access_token");
      loginInstanceUrl = jsonObjectOfResponse.getString("instance_url");
      logger.info("Login is Successful. Response Status: " + statusCode);
      logger.info("Instance URL: " + loginInstanceUrl);
      logger.info("Access token/session ID: " + loginAccessToken);
    } catch (IOException ioException) {
      logger.info("Error in establishing connection to Salesforce: " + ioException);
    } catch (JSONException jsonException) {
      logger.info("Error in processing the Response in JSON: " + jsonException);
    }

    return loginAccessToken;
  }

  public static String createObject(JSONObject objectJson, String objectName) throws UnsupportedEncodingException {
    getAccessToken();
    String baseUri = loginInstanceUrl + REST_ENDPOINT + API_VERSION;
    Header oauthHeader = new BasicHeader("Authorization", "Bearer " + loginAccessToken);
    String uri = baseUri + "/sobjects/" + objectName + "/";
    uniqueRecordId = "null";

    logger.info("JSON for  record to be inserted:\n" + objectJson.toString(1));

    HttpClient httpClient = HttpClientBuilder.create().build();
    HttpPost httpPost = new HttpPost(uri);
    httpPost.addHeader(oauthHeader);
    httpPost.addHeader(prettyPrintHeader);
    StringEntity body = new StringEntity(objectJson.toString(1));
    body.setContentType("application/json");
    httpPost.setEntity(body);

    try {
      HttpResponse response = httpClient.execute(httpPost);
      final int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 201) {
        String responseAsString = EntityUtils.toString(response.getEntity());
        JSONObject json = new JSONObject(responseAsString);
        uniqueRecordId = json.getString("id");
        logger.info("New Object id from response: " + uniqueRecordId);
      } else {
        logger.info("Insertion unsuccessful. Status code is: " + statusCode);
      }
    } catch (IOException ioException) {
      logger.info("Error in establishing connection to Salesforce: " + ioException);
    }

    return uniqueRecordId;
  }

  public static void queryObject(String id, String objectName) {
    getAccessToken();
    HttpClient httpClient = HttpClientBuilder.create().build();
    String baseUri = loginInstanceUrl + REST_ENDPOINT + API_VERSION;
    String uri = baseUri + "/sobjects/" + objectName + "/" + id;
    HttpGet httpGet = new HttpGet(uri);
    Header oauthHeader = new BasicHeader("Authorization", "Bearer " + loginAccessToken);
    httpGet.addHeader(oauthHeader);
    httpGet.addHeader(prettyPrintHeader);

    try {
      HttpResponse response = httpClient.execute(httpGet);
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 200) {
        String responseString = EntityUtils.toString(response.getEntity());
        Gson gson = new Gson();
        JsonObject objectResponseInJson = gson.fromJson(responseString, JsonObject.class);
        sobjectResponse.add(objectResponseInJson);
      }
    } catch (IOException ioException) {
      logger.info("Error in establishing connection to Salesforce: " + ioException);
    }
  }

  public static void deletePushTopic(String pushTopicName) {
    try {
      PartnerConnection partnerConnection = new PartnerConnection(
        Authenticator.createConnectorConfig(new AuthenticatorCredentials(USERNAME, PASSWORD + SECURITYTOKEN,
          CLIENTID, CLIENTSECRET, PluginPropertyUtils.pluginProp("login.url"),
          30000, "")));

      QueryResult queryResult = SalesforceStreamingSourceConfig.runQuery(
        partnerConnection,
        String.format("SELECT Id FROM PushTopic WHERE Name = '%s'", pushTopicName)
      );

      SObject sobject = queryResult.getRecords()[0];
      String pushTopicId = sobject.getField("Id").toString();
      DeleteResult[] deleteResults = partnerConnection.delete(new String[]{pushTopicId});

      // Check the result of the delete operation
      if (deleteResults != null && deleteResults.length > 0) {
        if (deleteResults[0].getSuccess()) {
          logger.info("Push Topic deleted successfully : " + pushTopicName);
        } else if (deleteResults[0].getErrors().length > 0) {
          logger.info("Error deleting Push Topic: " + deleteResults[0].getErrors()[0].getMessage());
        }
      }

    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new InvalidStageException(
        String.format("Cannot connect to Salesforce API with credentials specified due to error: %s", message), e);
    }
  }

  public static void deleteId(String id, String objectName) {
    getAccessToken();
    HttpClient httpClient = HttpClientBuilder.create().build();
    String baseUri = loginInstanceUrl + REST_ENDPOINT + API_VERSION;
    String uri = baseUri + "/sobjects/" + objectName + "/" + id;
    HttpDelete httpDelete = new HttpDelete(uri);
    Header oauthHeader = new BasicHeader("Authorization", "Bearer " + loginAccessToken);
    httpDelete.addHeader(oauthHeader);

    try {
      HttpResponse response = httpClient.execute(httpDelete);
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 204) {
        // Deletion successful
        logger.info("Id deleted successfully: " + id);
      } else {
        // Handle other status codes or error scenarios
        logger.info("Failed to delete Id.");
      }
    } catch (IOException ioException) {
      logger.info("Error in establishing connection to Salesforce: " + ioException);
    }
  }

  public static String queryObjectId(String objectName) {
    getAccessToken();
    HttpClient httpClient = HttpClientBuilder.create().build();
    String baseUri = loginInstanceUrl + REST_ENDPOINT + API_VERSION;

    try {
      String query = "SELECT Id FROM " + objectName;
      String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8.toString());
      String uri = baseUri + "/query?q=" + encodedQuery;

      HttpGet httpGet = new HttpGet(uri);
      Header oauthHeader = new BasicHeader("Authorization", "Bearer " + loginAccessToken);
      httpGet.addHeader(oauthHeader);
      httpGet.addHeader(prettyPrintHeader);

      HttpResponse response = httpClient.execute(httpGet);
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 200) {
        String responseString = EntityUtils.toString(response.getEntity());
        Gson gson = new Gson();
        JsonObject queryResponse = gson.fromJson(responseString, JsonObject.class);

        JsonArray records = queryResponse.getAsJsonArray("records");
        for (JsonElement record : records) {
          JsonObject recordObject = record.getAsJsonObject();
          uniqueRecordId = recordObject.get("Id").getAsString();
          logger.info("Queried Object id from response: " + uniqueRecordId);
        }
      }
    } catch (IOException ioException) {
      logger.info("Error in establishing connection to Salesforce: " + ioException);
    }
    return uniqueRecordId;
  }
}
