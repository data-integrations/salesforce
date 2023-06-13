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


package io.cdap.plugin.tests.hooks;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.salesforcestreamingsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.SalesforceClient;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Represents Test Setup and Clean up hooks.
 */
public class TestSetupHooks {

  @Before(order = 2, value = "@CREATE_TEST_DATA")
  public static void createTestObject() throws UnsupportedEncodingException, ParseException {
    JSONObject testData = new JSONObject(PluginPropertyUtils.pluginProp("testData"));
    SalesforceClient.createObject(testData, SObjects.AUTOMATION_CUSTOM__C.value);
    BeforeActions.scenario.write("Creating new Custom Object.." + PluginPropertyUtils.pluginProp("testData")
                                   + "created successfully");

  }

  @Before(order = 2, value = "@CREATE_TEST_DATA2")
  public static void createTestObject2() throws UnsupportedEncodingException, ParseException {
     BeforeActions.scenario.write("Creating new Custom Object..");
    JSONObject testObjectData = new JSONObject(PluginPropertyUtils.pluginProp("testObjectData"));
    SalesforceClient.createObject(testObjectData, SObjects.AUTOMATION_CUSTOM2__C.value);
    BeforeActions.scenario.write("Creating new Custom Object.." + PluginPropertyUtils.pluginProp("testObjectData")
                                   + "created successfully");
  }

  @After(order = 2, value = "@DELETE_TEST_DATA")
  public static void deleteTestObject() {
    String uniqueRecordId = SalesforceClient.queryObjectId(SObjects.AUTOMATION_CUSTOM__C.value);
    SalesforceClient.deleteId(uniqueRecordId, SObjects.AUTOMATION_CUSTOM__C.value);
    BeforeActions.scenario.write("Record - " + uniqueRecordId + " deleted successfully");
  }

  @After(order = 2, value = "@DELETE_TEST_DATA2")
  public static void deleteTestObject2() {
    String uniqueRecordId = SalesforceClient.queryObjectId(SObjects.AUTOMATION_CUSTOM2__C.value);
    SalesforceClient.deleteId(uniqueRecordId, SObjects.AUTOMATION_CUSTOM2__C.value);
    BeforeActions.scenario.write("Record - " + uniqueRecordId + " deleted successfully");
  }


  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFile"),
                                   PluginPropertyUtils.pluginProp("InsertBQDataQueryFile"));
  }

  @After(order = 1, value = "@BQ_TEMP_CLEANUP")
  public static void deleteTemperoryCreatedBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("bqTargetTable") + "_v1";
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("bqTargetTable");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table with 3 columns (FirstName - string, LastName - string, Company - string) containing random
   * test data.
   *
   * Sample row:
   * FirstName | LastName | Company                               |
   *     22    | 968      | 245308db-6088-4db2-a933-f0eea650846a  |
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST_LEAD")
  public static void createTempSourceBQTableForLeadSObject() throws IOException, InterruptedException {
    String uniqueId = RandomStringUtils.randomAlphanumeric(7);
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    String bqSourceDataset = PluginPropertyUtils.pluginProp("dataset");

    String firstName = "testLeadF_" + uniqueId;
    String lastName = "testLeadL_" + uniqueId;
    String company = uniqueId + ".com";


    BigQueryClient.getSoleQueryResult(
      "create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT('" + firstName + "' AS FirstName, '" +
        lastName + "' AS LastName, '" +
        company + "' AS Company)])"
    );
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");
    BigQueryClient.dropBqQuery(bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    PluginPropertyUtils.removePluginProp("bqSourceTable");
  }

  private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile, String bqInsertDataQueryFile)
    throws IOException, InterruptedException {
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");

    String createTableQuery = StringUtils.EMPTY;
    try {
      createTableQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqCreateTableQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      createTableQuery = createTableQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqCreateTableQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading create table query file " + e.getMessage());
    }

    String insertDataQuery = StringUtils.EMPTY;
    try {
      insertDataQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqInsertDataQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      insertDataQuery = insertDataQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqInsertDataQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading insert data query file " + e.getMessage());
    }
    BigQueryClient.getSoleQueryResult(createTableQuery);
    try {
      BigQueryClient.getSoleQueryResult(insertDataQuery);
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@CONNECTION")
  public static void setNewConnectionName() {
    String connectionName = "SalesforceConnection" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("connection.name", connectionName);
    BeforeActions.scenario.write("New Connection name: " + connectionName);
  }

  @After(order = 1, value = "@DELETE_PUSH_TOPIC")
  public static void deletePushTopic() {
    String pushTopicName = SalesforcePropertiesPageActions.topicName;
    SalesforceClient.deletePushTopic(pushTopicName);
    BeforeActions.scenario.write("PushTopic - " + pushTopicName + " deleted successfully");
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("bqTargetTable");
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("bqTargetTable");

    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public void setTempTargetBQTable() {
    String bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTable);
    BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
  }

  @After(order = 1, value = "@BQ_SINK_MULTI_TEST")
  public static void deleteTempTargetMultiBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("customTable1");
    String bqTargetTableName2 = PluginPropertyUtils.pluginProp("customTable2");

    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      BigQueryClient.dropBqQuery(bqTargetTableName2);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName2 + " deleted successfully");

    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName2 + " does not exist");

      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
