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
import io.cdap.plugin.utils.SalesforceClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Represents Test Setup and Clean up hooks.
 */
public class TestSetupHooks {
  public static String bqTargetDataset = StringUtils.EMPTY;
  public static String bqSourceTable = StringUtils.EMPTY;
  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceDataset = "sfdataset";

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQDataset() {
    bqTargetDataset = "TestSN_dataset" + RandomStringUtils.randomAlphanumeric(10);
    BeforeActions.scenario.write("BigQuery Target dataset name: " + bqTargetDataset);
  }

  @Before(order = 2, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTable() {
    bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
  }

  @Before(order = 3, value = "@BQ_SINK_TEST")
  public static void createLead() throws UnsupportedEncodingException {
    BeforeActions.scenario.write("Creating new Lead..");
    JSONObject lead = new JSONObject();
    String uniqueId = RandomStringUtils.randomAlphanumeric(10);
    lead.put("FirstName", "LFname_" + uniqueId);
    lead.put("LastName", "LLname_" + uniqueId);
    lead.put("Company", uniqueId + ".com");

    SalesforceClient.createLead(lead);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    try {
      BigQueryClient.dropBqQuery(bqTargetDataset, bqTargetTable);
      BeforeActions.scenario.write("BigQuery Target table: " + bqTargetTable + " is deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BigQuery Target Table: " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table with 3 columns (FirstName - string, LastName - string, Company - string) containing random
   * test data.
   * Sample row:
   * FirstName | LastName | Company
   * 22 | 968   | 245308db-6088-4db2-a933-f0eea650846a
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTableForLeadSObject() throws IOException, InterruptedException {
    String uniqueId = RandomStringUtils.randomAlphanumeric(7);
    bqSourceTable = "testTable" + uniqueId;
    String firstName = "testLeadF_" + uniqueId;
    String lastName = "testLeadL_" + uniqueId;
    String company = uniqueId + ".com";

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
      "SELECT * FROM UNNEST([ STRUCT('" + firstName + "' AS FirstName, '" + lastName + "' AS LastName, '"
      + company + "' AS Company)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    BigQueryClient.dropBqQuery(bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    bqSourceTable = StringUtils.EMPTY;
  }
}
