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

package io.cdap.plugin.utils;

import io.cdap.e2e.utils.PluginPropertyUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Represents Salesforce Client.
 */
public class SalesforceClient {
  private static final Logger logger = LoggerFactory.getLogger(SalesforceClient.class);
  private static final String USERNAME = PluginPropertyUtils.pluginProp("admin.username");
  private static final String PASSWORD = PluginPropertyUtils.pluginProp("admin.password");
  private static final String TOKEN_URL =  PluginPropertyUtils.pluginProp("login.url");
  private static final String GRANTTYPE = PluginPropertyUtils.pluginProp("grant.type");
  private static final String CLIENTID = PluginPropertyUtils.pluginProp("admin.consumer.key");
  private static final String CLIENTSECRET = PluginPropertyUtils.pluginProp("admin.consumer.secret");
  private static final String REST_ENDPOINT = PluginPropertyUtils.pluginProp("rest.api.endpoint");
  private static final String API_VERSION = PluginPropertyUtils.pluginProp("rest.api.version");
  private static final Header prettyPrintHeader = new BasicHeader("X-PrettyPrint", "1");
  private static String loginAccessToken = null;
  private static String loginInstanceUrl = null;

  public static String getAccessToken() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    final List<NameValuePair> loginParams = new ArrayList<>();
    loginParams.add(new BasicNameValuePair("client_id", CLIENTID));
    loginParams.add(new BasicNameValuePair("client_secret", CLIENTSECRET));
    loginParams.add(new BasicNameValuePair("grant_type", GRANTTYPE));
    loginParams.add(new BasicNameValuePair("username", USERNAME));
    loginParams.add(new BasicNameValuePair("password", PASSWORD));

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

  public static String createLead(JSONObject leadObject) throws UnsupportedEncodingException {
    getAccessToken();
    String baseUri = loginInstanceUrl + REST_ENDPOINT + API_VERSION;
    Header oauthHeader = new BasicHeader("Authorization", "Bearer " + loginAccessToken);
    String uri = baseUri + "/sobjects/Lead/";
    String leadId = null;
    logger.info("JSON for Lead record to be inserted:\n" + leadObject.toString(1));

    HttpClient httpClient = HttpClientBuilder.create().build();
    HttpPost httpPost = new HttpPost(uri);
    httpPost.addHeader(oauthHeader);
    httpPost.addHeader(prettyPrintHeader);
    StringEntity body = new StringEntity(leadObject.toString(1));
    body.setContentType("application/json");
    httpPost.setEntity(body);

    try {
      HttpResponse response = httpClient.execute(httpPost);
      final int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 201) {
        String responseAsString = EntityUtils.toString(response.getEntity());
        JSONObject json = new JSONObject(responseAsString);
        leadId = json.getString("id");
        logger.info("New Lead id from response: " + leadId);
      } else {
        logger.info("Insertion unsuccessful. Status code is: " + statusCode);
      }
    } catch (IOException ioException) {
      logger.info("Error in establishing connection to Salesforce: " + ioException);
    } catch (JSONException jsonException) {
      logger.info("Error in processing the Response in JSON: " + jsonException);
    }

    return leadId;
  }
}
