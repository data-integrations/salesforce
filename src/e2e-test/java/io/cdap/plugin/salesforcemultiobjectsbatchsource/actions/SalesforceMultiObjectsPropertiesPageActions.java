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

package io.cdap.plugin.salesforcemultiobjectsbatchsource.actions;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;

import io.cdap.plugin.salesforcemultiobjectsbatchsource.locators.SalesforceMultiObjectsPropertiesPage;
import io.cdap.plugin.utils.enums.SObjects;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Salesforce MultiObjects batch source plugin - Actions.
 */
public class SalesforceMultiObjectsPropertiesPageActions {
  private static Gson gson = new Gson();
  private static List<String> bigQueryrows = new ArrayList<>();
  static {
    SeleniumHelper.getPropertiesLocators(SalesforceMultiObjectsPropertiesPage.class);
  }

  public static void fillWhiteListWithSObjectNames(List<SObjects> sObjectNames) {
    int totalSObjects = sObjectNames.size();

    for (int i = 0; i < totalSObjects - 1; i++) {
      SalesforceMultiObjectsPropertiesPage.sObjectNameAddRowButtonsInWhiteList.get(i).click();
    }

    for (int i = 0; i < totalSObjects; i++) {
      ElementHelper.sendKeys(SalesforceMultiObjectsPropertiesPage.sObjectNameInputsInWhiteList.get(i),
              sObjectNames.get(i).value);
    }
  }

  public static void fillBlackListWithSObjectNames(List<SObjects> sObjectNames) {
    int totalSObjects = sObjectNames.size();

    for (int i = 0; i < totalSObjects - 1; i++) {
      SalesforceMultiObjectsPropertiesPage.sObjectNameAddRowButtonsInBlackList.get(i).click();
    }

    for (int i = 0; i < totalSObjects; i++) {
      ElementHelper.sendKeys(SalesforceMultiObjectsPropertiesPage.sObjectNameInputsInBlackList.get(i),
              sObjectNames.get(i).value);
    }
  }

  public static void verifyInvalidSObjectNameValidationMessageForWhiteList(List<SObjects> whiteListedSObjects) {
    String invalidSObjectNameValidationMessage = PluginPropertyUtils.errorProp("invalid.sobjectnames");

    if (whiteListedSObjects.size() > 0) {
      AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInWhiteList,
        invalidSObjectNameValidationMessage);

      for (SObjects whiteListedSObject : whiteListedSObjects) {
        AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInWhiteList,
          whiteListedSObject.value);
      }
    }
  }

  public static void verifyInvalidSObjectNameValidationMessageForBlackList(List<SObjects> blackListedSObjects) {
    String invalidSObjectNameValidationMessage = PluginPropertyUtils.errorProp("invalid.sobjectnames");

    if (blackListedSObjects.size() > 0) {
      AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInBlackList,
        invalidSObjectNameValidationMessage);

      for (SObjects blackListedSObject : blackListedSObjects) {
        AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInBlackList,
          blackListedSObject.value);
      }
    }
  }

  public static void verifyIfRecordCreatedInSinkForObjectsAreCorrect(String expectedOutputFile)
    throws IOException, InterruptedException {
    List<String> expectedOutput = new ArrayList<>();
    try (BufferedReader bf1 = Files.newBufferedReader(Paths.get(PluginPropertyUtils.pluginProp(expectedOutputFile)))) {
      String line;
      while ((line = bf1.readLine()) != null) {
        expectedOutput.add(line);
      }
    }
    List<String> bigQueryDatasetTables = new ArrayList<>();
    TableResult tablesSchema = getTableNamesFromDataSet(PluginPropertyUtils.pluginProp("dataset"));
    tablesSchema.iterateAll().forEach(value -> bigQueryDatasetTables.add(value.get(0).getValue().toString()));

//    createNewTableFromQuery(PluginPropertyUtils.pluginProp("dataset"),
//                            PluginPropertyUtils.pluginProp("bqTargetTable"));

    for (int expectedRow = 0; expectedRow < expectedOutput.size(); expectedRow++) {
      JsonObject expectedOutputAsJson = gson.fromJson(expectedOutput.get(expectedRow), JsonObject.class);
      String uniqueId = expectedOutputAsJson.get("Id").getAsString();
      getBigQueryTableData(PluginPropertyUtils.pluginProp("dataset"),
                           bigQueryDatasetTables.get(0), uniqueId);

    }
//    for (int row = 0; row < bigQueryrows.size() && row < expectedOutput.size(); row++) {
//      Assert.assertTrue(SalesforcePropertiesPageActions.compareValueOfBothResponses(
//        expectedOutput.get(row), bigQueryrows.get(row)));
//    }
  }

  private static TableResult getTableNamesFromDataSet(String bqTargetDataset) throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT table_name FROM `" + projectId + "." + bqTargetDataset +
      "`.INFORMATION_SCHEMA.TABLES ";

    return BigQueryClient.getQueryResult(selectQuery);
  }

  private static void getBigQueryTableData(String dataset, String table, String uniqueId)
    throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t WHERE " +
      "Id='" + uniqueId + "' ";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryrows.add(value.get(0).getValue().toString()));
  }
}
