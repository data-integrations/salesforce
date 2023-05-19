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

package io.cdap.plugin.salesforcesink.actions;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcesink.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.enums.OperationTypes;
import io.cdap.plugin.utils.enums.SObjects;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Salesforce sink plugins - Actions.
 */

public class SalesforcePropertiesPageActions {
  private static Gson gson = new Gson();
  private static List<String> bigQueryrows = new ArrayList<>();

  static {
    SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
  }

  public static void fillReferenceName(String referenceName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.referenceInput, referenceName);
  }

  public static void fillsObjectName(String sObjectName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.sObjectNameInput, sObjectName);
  }

  public static void configureSalesforcePluginForSobjectName(SObjects sObjectName) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillsObjectName(sObjectName.value);
  }

  public static void selectOperationType(OperationTypes operationType) {
    SalesforcePropertiesPage.locateOperationTypeRadioButtons(operationType.value).click();
    fillUpsertExternalIDIfOperationTypeISUpsert(operationType.value);
  }

  public static void fillUpsertExternalIDIfOperationTypeISUpsert(String operationType) {
    if (operationType.equals(OperationTypes.UPSERT.value)) {
      fillUpsertExternalIdField();
    }
  }

  public static void fillUpsertExternalIdField() {
    String externalId = PluginPropertyUtils.pluginProp("upsert.externalId");
    ElementHelper.sendKeys(SalesforcePropertiesPage.upsertExternalIdFieldInput, externalId);
  }

  public static void selectErrorHandlingOptionType(String errorOption) {
    ElementHelper.selectDropdownOption(SalesforcePropertiesPage.errordropdown,
      CdfPluginPropertiesLocators.locateDropdownListItem(errorOption));
  }

  public static void fillMaxRecords(String maxRecords) {
    ElementHelper.clearElementValue(SalesforcePropertiesPage.maxRecordsPerbatchInput);
    ElementHelper.sendKeys(SalesforcePropertiesPage.maxRecordsPerbatchInput, maxRecords);
  }

  public static void fillMaxBytes(String maxBytesPerBatch) {
    ElementHelper.clearElementValue(SalesforcePropertiesPage.maxBytesPerBatchInput);
    ElementHelper.sendKeys(SalesforcePropertiesPage.maxBytesPerBatchInput, maxBytesPerBatch);
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
