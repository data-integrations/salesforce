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

package io.cdap.plugin.servicenowsink.actions;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.locators.ServiceNowPropertiesPage;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.tests.hooks.TestSetupHooks;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.junit.Assert;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * ServiceNow sink - Properties page - Actions.
 */

public class ServiceNowSinkPropertiesPageActions {
  public static ServiceNowSourceConfig config;
  private static Map<String, String> responseFromServiceNowTable;
  private static Gson gson = new Gson();

  public static void getRecordFromServiceNowTable(String query, String tableName)
      throws ServiceNowAPIException {
    config = new ServiceNowSourceConfig(
        "", "", "", "", "",
        System.getenv("SERVICE_NOW_CLIENT_ID"),
        System.getenv("SERVICE_NOW_CLIENT_SECRET"),
        System.getenv("SERVICE_NOW_REST_API_ENDPOINT"),
        System.getenv("SERVICE_NOW_USERNAME"),
        System.getenv("SERVICE_NOW_PASSWORD"),
        "", "", "", null);

    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection());
    responseFromServiceNowTable = tableAPIClient.getRecordFromServiceNowTable(tableName, query);
  }

  public static void verifyIfRecordCreatedInServiceNowIsCorrect(String query, String tableName)
      throws IOException, InterruptedException, ServiceNowAPIException {

    getRecordFromServiceNowTable(query, tableName);
    TableResult bigQueryTableData = getBigQueryTableData(TestSetupHooks.bqSourceDataset, TestSetupHooks.bqSourceTable);
    if (bigQueryTableData == null) {
      return;
    }
    String bigQueryJsonResponse = bigQueryTableData.getValues().iterator().next().get(0).getValue().toString();
    JsonObject jsonObject = gson.fromJson(bigQueryJsonResponse, JsonObject.class);
    Map<String, Object> bigQueryResponseInMap = gson.fromJson(jsonObject.toString(), Map.class);
    Assert.assertTrue(compareValueOfBothResponses(responseFromServiceNowTable, bigQueryResponseInMap));
  }

  public static void verifyIfRecordUpdatedInServiceNowIsCorrect(String query, String tableName)
      throws IOException, InterruptedException, ServiceNowAPIException {

    getRecordFromServiceNowTable(query, tableName);
    TableResult bigQueryTableData = getBigQueryTableData(TestSetupHooks.bqSourceDataset, TestSetupHooks.bqSourceTable);
    if (bigQueryTableData == null) {
      return;
    }
    String bigQueryJsonResponse = bigQueryTableData.getValues().iterator().next().get(0).getValue().toString();
    JsonObject jsonObject = gson.fromJson(bigQueryJsonResponse, JsonObject.class);
    String bigQuerySystemId = jsonObject.get(ServiceNowConstants.SYSTEM_ID).getAsString();
    JsonObject serviceNowJson = ServiceNowTableAPIClientImpl.serviceNowJsonResultArray.get(0).getAsJsonObject();
    String serviceNowSystemId = serviceNowJson.get(ServiceNowConstants.SYSTEM_ID).getAsString();

    Assert.assertTrue(bigQuerySystemId.equals(serviceNowSystemId));
  }

  public static TableResult getBigQueryTableData(String dataset, String table)
      throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) result FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    return BigQueryClient.getQueryResult(selectQuery);
  }

  public static boolean compareValueOfBothResponses(Map<String, String> serviceNowResponseMap,
      Map<String, Object> bigQueryResponseMap) {
    if (serviceNowResponseMap.isEmpty() || bigQueryResponseMap.isEmpty()) {
      return false;
    }
    boolean result = false;
    Set<String> bigQueryKeySet = bigQueryResponseMap.keySet();

    for (String key : bigQueryKeySet) {
      Object serviceNowValue = serviceNowResponseMap.get(key);
      Object bigQueryValue = bigQueryResponseMap.get(key);

      if (bigQueryValue instanceof Double) {
        String bigDecimalValue = new BigDecimal(String.valueOf(bigQueryValue)).setScale(
            ServiceNowConstants.DEFAULT_SCALE, RoundingMode.HALF_UP).toString();
        result = serviceNowValue.equals(bigDecimalValue);
      } else if (checkBigQueryDateFormat(bigQueryValue.toString()) != null) {
        SimpleDateFormat serviceNowDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String bigQueryFormattedValue = serviceNowDateFormat.format(checkBigQueryDateFormat(bigQueryValue.toString()));
        result = String.valueOf(serviceNowValue).equals(bigQueryFormattedValue);
      } else {
        result = String.valueOf(serviceNowValue).equals(String.valueOf(bigQueryValue));
      }

      if (!result) {
        return false;
      }
    }
    return result;
  }

  /**
   * Return BigQuery supported date formats.
   */
  @Nullable
  private static Date checkBigQueryDateFormat(String value) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
    SimpleDateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
    Date date;
    try {
      date = dateFormat.parse(value);
      return date;
    } catch (ParseException ignored) {
    }
    try {
      date = dateTimeFormat.parse(value);
      return date;
    } catch (ParseException ignored) {
    }
    try {
      date = timeStampFormat.parse(value);
      return date;
    } catch (ParseException ignored) {
    }
    return null;
  }

  public static void verifyErrorForNonCreatableFields() {
    AssertionHelper.verifyElementDisplayed(ServiceNowPropertiesPage.fieldNotCreatableError);
  }
}
