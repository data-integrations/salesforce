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

package io.cdap.plugin.tests.hooks;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.utils.enums.ApplicationInReportingMode;
import io.cdap.plugin.utils.enums.TablesInTableMode;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.StringEntity;
import org.junit.Assert;
import stepsdesign.BeforeActions;
import java.io.IOException;
import java.util.Random;

/**
 * Represents Test Setup and Clean up hooks.
 */
public class TestSetupHooks {
  public static ServiceNowSourceConfig config;
  public static String bqTargetDataset = "test_automation";
  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceDataset = "test_automation";
  public static String bqSourceTable;
  public static String systemId;
  public static String receivingSlipLineRecordUniqueNumber;
  public static String agentAssistRecommendationUniqueName;
  public static String vendorCatalogItemUniqueName;
  public static String serviceOfferingUniqueNumber;

  @Before(order = 1, value = "@SN_SOURCE_CONFIG")
  public static void initializeServiceNowSourceConfig() {
    BeforeActions.scenario.write("Initialize ServiceNowSourceConfig");
    config = new ServiceNowSourceConfig(
        "", "", "", "", "",
        System.getenv("SERVICE_NOW_CLIENT_ID"),
        System.getenv("SERVICE_NOW_CLIENT_SECRET"),
        System.getenv("SERVICE_NOW_REST_API_ENDPOINT"),
        System.getenv("SERVICE_NOW_USERNAME"),
        System.getenv("SERVICE_NOW_PASSWORD"),
        "", "", "", null);
  }

  @Before(order = 2, value = "@SN_PRODUCT_CATALOG_ITEM")
  public static void createRecordInProductCatalogItemTable() throws IOException, ServiceNowAPIException {
    BeforeActions.scenario.write("Create new record in Product Catalog Item table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection());
    String uniqueId = "TestProductCatalogItem" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'name':'" + uniqueId + "','price':'2500'}";
    StringEntity entity = new StringEntity(recordDetails);
    tableAPIClient.createRecord(ApplicationInReportingMode.PRODUCT_CATALOG.value, entity);
  }

  @Before(order = 2, value = "@SN_RECEIVING_SLIP_LINE")
  public static void createRecordInReceivingSlipLineTable()
      throws IOException, ServiceNowAPIException {
    BeforeActions.scenario.write("Create new record in Receiving Slip Line table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection());
    String uniqueId = "TestReceivingSlipLine" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'number':'" + uniqueId + "'}";
    StringEntity entity = new StringEntity(recordDetails);
    systemId = tableAPIClient.createRecord(TablesInTableMode.RECEIVING_SLIP_LINE.value, entity);
  }

  @Before(order = 2, value = "@SN_UPDATE_AGENT_ASSIST_RECOMMENDATION")
  public static void updateRecordInAgentAssistRecommendationTable()
      throws IOException, ServiceNowAPIException {
    BeforeActions.scenario.write("Create new record in Agent Assist Recommendation table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection());
    String uniqueId = "TestAgentAssist" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'active':'false','name':'" + uniqueId + "'}";
    StringEntity entity = new StringEntity(recordDetails);
    systemId = tableAPIClient.createRecord(TablesInTableMode.AGENT_ASSIST_RECOMMENDATION.value, entity);
  }

  @Before(order = 2, value = "@SN_UPDATE_VENDOR_CATALOG_ITEM")
  public static void updateRecordInAgentVendorCatalogItem()
      throws IOException, ServiceNowAPIException {
    BeforeActions.scenario.write("Create new record in Vendor Catalog Item table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection());
    String uniqueId = "TestVendorCatalog" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'out_of_stock':'false','product_id':'" + uniqueId + "'}";
    StringEntity entity = new StringEntity(recordDetails);
    systemId = tableAPIClient.createRecord(TablesInTableMode.VENDOR_CATALOG_ITEM.value, entity);
  }

  @Before(order = 2, value = "@SN_UPDATE_SERVICE_OFFERING")
  public static void updateRecordInServiceOffering() throws IOException, ServiceNowAPIException {
    BeforeActions.scenario.write("Create new record in Service Offering table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection());
    String uniqueId = "TestServiceOffering" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'purchase_date':'2022-05-28','end_date':'2022-06-05 15:00:00'," +
        " 'start_date':'2022-05-25 15:00:00','number':'" + uniqueId + "'}";
    StringEntity entity = new StringEntity(recordDetails);
    systemId = tableAPIClient.createRecord(TablesInTableMode.SERVICE_OFFERING.value, entity);
  }

  @Before(order = 1, value = "@BQ_SOURCE_TEST_RECEIVING_SLIP_LINE")
  public static void createTempSourceBQTableForReceivingSlipLineTable() throws IOException, InterruptedException {
    Random uniqueId = new Random();
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;
    receivingSlipLineRecordUniqueNumber = "ProcRecSlip" + stringUniqueId;

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT('" + receivingSlipLineRecordUniqueNumber + "' " +
        "AS number, (DATETIME '2022-06-08 00:00:00')  AS received)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_AGENT_ASSIST_RECOMMENDATION")
  public static void createTempSourceBQTableForAgentFile() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;
    boolean active = true;
    agentAssistRecommendationUniqueName = "Agent" + stringUniqueId;

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT(" + active + " AS active," +
        " '" + agentAssistRecommendationUniqueName + "'  AS name)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_VENDOR_CATALOG_ITEM")
  public static void createTempSourceBQTableForAgentAssistRecomendation() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    Random uniqueId = new Random();
    bqSourceTable = "testTable" + stringUniqueId;

    float listPrice = uniqueId.nextFloat();
    double price = uniqueId.nextDouble();
    boolean outOfStock = true;
    vendorCatalogItemUniqueName = "VendorCatalog" + stringUniqueId;

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT(" + outOfStock + " AS out_of_stock,' "
        + vendorCatalogItemUniqueName + " ' AS product_id)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_SERVICE_OFFERING")
  public static void createTempSourceBQTableForServiceOffering() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;
    serviceOfferingUniqueNumber = "ServiceOffering" + stringUniqueId;
    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT( (DATE '2022-06-10') AS purchase_date," +
        " (DATETIME '2022-06-08 16:00:00') AS end_date," +
        " (TIMESTAMP '2022-05-10 15:00:00-00:00') AS start_date,' "
        + serviceOfferingUniqueNumber + " ' AS number)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 3, value = "@BQ_SOURCE_UPDATE_RECEIVING_SLIP_LINE")
  public static void updateTempSourceBQTableForReceivingSlipLineTable() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;
    Random uniqueId = new Random();
    int updates = 2;
    String number = "updatedReceiving" + uniqueId;

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT('" + number + "'  AS number," +
        " '" + systemId + "' AS sys_id )])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 3, value = "@BQ_SOURCE_UPDATE_AGENT_ASSIST_RECOMMENDATION")
  public static void updateTempSourceBQTableForAgentFile() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;
    boolean active = true;
    String name = "Agent";

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT(" + active + " AS active," +
        " '" + name + "'  AS name," +
        " '" + systemId + "' AS sys_id)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 3, value = "@BQ_SOURCE_UPDATE_VENDOR_CATALOG_ITEM")
  public static void createTempSourceBQTableForVendorCatalogItem() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    Random uniqueId = new Random();
    bqSourceTable = "testTable" + stringUniqueId;
    float listPrice = uniqueId.nextFloat();
    boolean outOfStock = true;
    String name = "check";

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT(" + outOfStock + " AS out_of_stock,'"
        + name + "' AS sys_update_name," +
        " '" + systemId + "' AS sys_id)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 3, value = "@BQ_SOURCE_UPDATE_SERVICE_OFFERING")
  public static void updateTempSourceBQTableForServiceOffering() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;
    serviceOfferingUniqueNumber = "ServiceOffering" + stringUniqueId;
    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
        "SELECT * FROM UNNEST([ STRUCT((DATE '2022-06-10') AS purchase_date," +
        " (DATETIME '2022-06-08 16:00:00') AS end_date," +
        " (TIMESTAMP '2022-05-10 15:00:00-00:00') AS start_date," +
        " '" + systemId + "' AS sys_id,' "
        + serviceOfferingUniqueNumber + " ' AS number)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 5, value = "@BQ_SINK")
  public static void setTempTargetBQTable() {
    bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
  }

  @Before(order = 1, value = "@CONNECTION")
  public static void setNewConnectionName() {
    String connectionName = "ServiceNowConnection" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("connection.name", connectionName);
    BeforeActions.scenario.write("New Connection name: " + connectionName);
  }

  @After(order = 1, value = "@BQ_SINK_CLEANUP")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    try {
      BigQueryClient.dropBqQuery(bqTargetTable);
      BeforeActions.scenario.write("BigQuery Target table: " + bqTargetTable + " is deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getCode() == 404) {
        BeforeActions.scenario.write("BigQuery Target Table: " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
